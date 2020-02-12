import time
import shutil
import logging
import datetime
from pathlib import Path
from argparse import ArgumentParser

import requests
from sqlalchemy.orm import sessionmaker, Session

import tweetarchiver
#from tweetarchiver.tests import test_live

WORKING_DIR = Path.home() / "tweetarchiver"
WORKING_DIR.mkdir(exist_ok=True)

# getLogger returns logger with different level and config than the one in __init__
# I'm not really sure why that happens
LOGGER = tweetarchiver.LOGGER

PARSER = ArgumentParser(
    prog="tweetarchiver",
    description="",
    epilog=""
)

PARSER.add_argument("username",
                    type=str, help="The account name whose tweets are to be archived")
PARSER.add_argument("--store-html",
                    action="store_true", help="Store tweets in html form in separate table -- this increases database size dramatically")
PARSER.add_argument("--skip-tests",
                    action="store_true", help="Do not perform initial scraper tests, which check whether scraping methods are up to date")
PARSER.add_argument("--skip-tweets",
                    action="store_true", help="Do not update tweets database")
PARSER.add_argument("--skip-images",
                    action="store_true", help="Do not download images")
PARSER.add_argument("--skip-videos",
                    action="store_true", help="Do not download videos")
PARSER.add_argument("--skip-media",
                    action="store_true", help="Do not download videos or images")
PARSER.add_argument("--skip-update",
                    action="store_true", help="Do not download tweets, videos or images")
PARSER.add_argument("--export",
                    type=Path, help="Export database contents to a csv file")
PARSER.add_argument("-v", "--version",
                    action="version", version="%(prog)s {}".format(tweetarchiver.__VERSION__))


def update_tweets(username: str, db_session: Session, store_html: bool = False) -> int:
    newest_id = tweetarchiver.Tweet.newest_tweet(db_session)
    oldest_id = tweetarchiver.Tweet.oldest_tweet(db_session)
    attachment_rows = 0
    tweet_rows = 0
    start_time = time.time()
    options = []
    if newest_id:
        # only get tweets older than what's already in db
        options.append({"max_id":oldest_id})
    if oldest_id:
        # only get newer stuff
        options.append({"min_id":newest_id})
    if not options:
        # get it all
        options = [{}]

    for kwargs in options:
        for html_page in tweetarchiver.scrape_tweets(username, **kwargs):
            timestamp = int(time.time())
            attachments = []
            tweets_html = []
            tweets_parsed = []

            for html in html_page:
                if store_html:
                    tweets_html.append(tweetarchiver.TweetHTML(html, timestamp))

                tweet_parsed = tweetarchiver.Tweet.from_html(html)
                if tweet_parsed.has_video or tweet_parsed.image_count:
                    attachments.extend(tweetarchiver.Attachment.from_html(html))

                tweets_parsed.append(tweet_parsed)

            db_session.add_all(tweets_html)
            db_session.add_all(tweets_parsed)
            db_session.add_all(attachments)

            db_session.commit()
            tweet_rows += len(tweets_parsed)
            attachment_rows += len(attachments)

    time_str = str(datetime.timedelta(seconds=time.time() - start_time))
    LOGGER.info("Inserted %s new tweet rows", tweet_rows)
    LOGGER.info("Inserted %s new attachment rows", attachment_rows)
    LOGGER.info("This took %s", time_str)
    return attachment_rows + tweet_rows


# decide the structure:
# one db per account, displaying full threads requires joining dbs
# one db per main account, context tweets from other accounts stored alongside
#
def update_media(db_session: Session, archive_dir: Path) -> int:
    LOGGER.debug("Starting update_media")
    dirs = {
        "attachments" : archive_dir / "attachments",
        "attachments/gifs" : archive_dir / "attachments" / "gif",
        "attachments/imgs" : archive_dir / "attachments" / "img",
        "attachments/vids" : archive_dir / "attachments" / "vid",
        "tmp" : archive_dir / "tmp",
    }

    for path in dirs.values():
        path.mkdir(exist_ok=True)

    downloaded = 0
    duplicates = 0
    for attachment in tweetarchiver.Attachment.with_missing_files(db_session):
        if attachment.type == "vid:mp4":
            LOGGER.warning("VIDEO DOWNLOAD NOT YET IMPLEMENTED, SKIPPING")
            continue

        filename = attachment.url.rsplit("/", maxsplit=1)[-1]
        temp_file = dirs["tmp"] / filename

        if attachment.type.startswith("img"):
            suffixes = [":orig", ":large", ""]
        else:
            suffixes = [""]

        file_download = None
        for suffix in suffixes:
            with temp_file.open(mode="wb") as download_destination:
                LOGGER.info("Downloading %s", filename)
                try:
                    file_download = tweetarchiver.download(
                        f"{attachment.url}{suffix}", to_file=download_destination)
                    break
                except requests.HTTPError as err:
                    if err.response.status_code == 404:
                        # continue down the suffix list
                        pass

                    print(f"Could not complete download due to HTTP error: {str(err)}")
                    raise
                except requests.RequestException as exc:
                    print(f"Could not complete download due to network error: {str(exc)}")
                    raise

        if not file_download:
            LOGGER.error("DOWNLOAD FAILED FOR URL:%s", attachment.url)
            continue

        matching_hash_query = db_session.query(tweetarchiver.Attachment).filter(tweetarchiver.Attachment.hash == file_download.hash)
        known_file = matching_hash_query.first()
        if known_file:
            duplicates += 1
            LOGGER.debug("Duplicate file found")
            LOGGER.debug("known url:%s, duplicate url:%s, hash:%s", known_file.url, attachment.url, file_download.hash)
            assert known_file.size == file_download.size
            attachment.size = known_file.size
            attachment.hash = known_file.hash
            attachment.path = known_file.path
            temp_file.unlink()
        else:
            downloaded += 1
            if attachment.type.startswith("img"):
                final_file_path = dirs["attachments/imgs"] / filename
            elif attachment.type == "vid:gif":
                final_file_path = dirs["attachments/gifs"] / filename
            else:
                final_file_path = dirs["attachments/vids"] / filename

            shutil.move(temp_file, final_file_path)
            attachment.size = file_download.size
            attachment.hash = file_download.hash
            attachment.path = str(final_file_path.relative_to(archive_dir))

        db_session.commit()

    LOGGER.info("Downloaded %s new attachments", downloaded)
    print(f"Downloaded {downloaded} new attachments")
    LOGGER.info("Skipped %s attachments with matching hashes", duplicates)
    print(f"Skipped {duplicates} attachments with matching hashes")
    return downloaded


def scraper_test() -> bool:
    print("Performing parser test on live data...", end="", flush=True)
    #test_live.livetest()
    print("Done!")
    return True


def full_test() -> bool:
    pass


def export(session: Session) -> str:
    pass


def main() -> None:
    args = PARSER.parse_args()

    if not args.skip_tests:
        scraper_test()

    username = args.username.lower()
    dbpath = WORKING_DIR / username
    dbpath.mkdir(exist_ok=True)
    dbfile = dbpath / f"{username}_twitter_archive.sqlite"
    dbexists = Path(dbpath).exists()

    sqla_engine = tweetarchiver.sqla.create_engine(f"sqlite:///{str(dbfile)}", echo=False)
    tweetarchiver.DeclarativeBase.metadata.create_all(sqla_engine)
    bound_session = sessionmaker(bind=sqla_engine)
    LOGGER.info("Creating new db session")
    session = bound_session()

    try:
        if not args.skip_update:
            if not args.skip_tweets:
                update_tweets(username, session)
            if not args.skip_media:
                update_media(session, dbpath)
        if args.export:
            export(session)
    except:
        LOGGER.error("Uncaught exception, rolling back db session")
        session.rollback()
        raise
    finally:
        LOGGER.info("Closing db session")
        session.close()


if __name__ == "__main__":
    LOG_FORMAT_FILE = logging.Formatter("[%(levelname)s] %(asctime)s: %(name)s.%(funcName)s() line:%(lineno)d %(message)s")
    FH = logging.FileHandler(WORKING_DIR / "lastrun.log", mode="w")
    FH.setLevel(logging.DEBUG)
    FH.setFormatter(LOG_FORMAT_FILE)
    LOGGER.addHandler(FH)
    try:
        main()
    except Exception as exc:
        # ignore argparse-issued systemexit
        if not isinstance(exc, SystemExit):
            LOGGER.exception("UNCAUGHT EXCEPTION")
            shutil.copy(FH.baseFilename, WORKING_DIR / time.strftime("exception_%Y-%m-%dT_%H-%M-%S.log"))
            raise
    finally:
        # ensure connection pool is cleared
        tweetarchiver.TWITTER_SESSION.close()

    # TODO: scrape the profile page for metadata
    # TODO: use account id instead of displayname for identifying accounts
    # TODO: account for possible changes of handle/displayname
    # TODO: save conversation context

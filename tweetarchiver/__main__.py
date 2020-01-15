import time
import shutil
from pathlib import Path
from argparse import ArgumentParser

from sqlalchemy.orm import sessionmaker, Session

import tweetarchiver

PARSER = ArgumentParser(
    prog="",
    description="",
    epilog=""
)

PARSER.add_argument(
    "username", type=str, help="The account name whose tweets are to be archived")
PARSER.add_argument(
    "--skip-tests", action="store_true", help="Do not perform initial scraper tests, which check whether scraping methods are up to date")
PARSER.add_argument(
    "--skip-tweets", action="store_true", help="Do not update tweets database")
PARSER.add_argument(
    "--skip-images", action="store_true", help="Do not download images")
PARSER.add_argument(
    "--skip-videos", action="store_true", help="Do not download videos")
PARSER.add_argument(
    "--skip-update", action="store_true", help="Do not download tweets, videos or images")
PARSER.add_argument(
    "--export", type=Path, help="Export database contents to a csv file")
PARSER.add_argument(
    "-v", "--version", action="version", version="%(prog)s {}".format(tweetarchiver.__VERSION__))

# getLogger returns logger with different level and config than the one in __init__
# I'm not really sure why that happens
LOGGER = tweetarchiver.LOGGER

WORKING_DIR = Path.home() / "tweetarchiver"
WORKING_DIR.mkdir(exist_ok=True)


def update_tweets(username: str, session: Session) -> int:
    newest_id = tweetarchiver.Tweet.newest_tweet(session)
    oldest_id = tweetarchiver.Tweet.oldest_tweet(session)
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
                tweets_html.append(tweetarchiver.TweetHTML(html, timestamp))
                tweet_parsed = tweetarchiver.Tweet(html)
                if tweet_parsed.has_video or tweet_parsed.image_count:
                    attachments.extend(tweetarchiver.Attachment.from_html(html))

                tweets_parsed.append(tweet_parsed)

            session.add_all(tweets_html)
            session.add_all(tweets_parsed)
            session.add_all(attachments)

            session.commit()
            tweet_rows += len(tweets_parsed)
            attachment_rows += len(attachments)

    time_spent_s = (time.time() - start_time)
    time_spent_m = time_spent_s // 60
    time_spent_s = time_spent_s % 60
    time_spent_h = time_spent_m // 60
    time_spent_m = time_spent_m % 60
    time_str = f"{time_spent_h:.0f}h {time_spent_m:.0f}m {time_spent_s:.0f}s"
    LOGGER.info("Inserted %s new tweet rows", tweet_rows)
    LOGGER.info("Inserted %s new attachment rows", attachment_rows)
    LOGGER.info("This took %s", time_str)
    return attachment_rows + tweet_rows


# decide the structure:
# one db per account, displaying full threads requires joining dbs
# one db per main account, context tweets from other accounts stored alongside
#
def update_media(session: Session, archive_dir: Path) -> int:
    attachments_path = archive_dir / "attachments"
    attachments_path.mkdir(exist_ok=True)
    gifs_path = attachments_path / "gif"
    gifs_path.mkdir(exist_ok=True)
    imgs_path = attachments_path / "img"
    imgs_path.mkdir(exist_ok=True)
    vids_path = attachments_path / "vid"
    vids_path.mkdir(exist_ok=True)
    tmpdir = archive_dir / "tmp"
    tmpdir.mkdir(exist_ok=True)

    pending_attachments = tweetarchiver.Attachment.with_missing_files(session)
    for attachment in pending_attachments:
        if attachment.type == "vid:mp4":
            LOGGER.warning("VIDEO DOWNLOAD CURRENTLY NOT IMPLEMENTED, SKIPPING")
            continue

        filename = attachment.url.rsplit("/", maxsplit=1)[-1]
        temp_download = tmpdir / filename
        if attachment.type.startswith("img"):
            url = f"{attachment.url}:orig"
        else:
            url = attachment.url

        with temp_download.open(mode="wb") as download_destination:
            md5sum = tweetarchiver.download(url, to_file=download_destination)

        downloaded_file_size = temp_download.stat().st_size

        matching_hash_query = session.query(tweetarchiver.Attachment).filter(tweetarchiver.Attachment.hash == md5sum)
        known_file = matching_hash_query.first()
        if known_file:
            LOGGER.debug("Duplicate file found")
            LOGGER.debug("known url:%s, duplicate url:%s, hash:%s", known_file.url, attachment.url, md5sum)
            assert known_file.size == downloaded_file_size
            attachment.size = known_file.size
            attachment.hash = md5sum
            attachment.path = known_file.path
            temp_download.unlink()
        else:
            if attachment.type.startswith("img"):
                final_file_path = imgs_path / filename
            elif attachment.type == "vid:gif":
                final_file_path = gifs_path / filename
            else:
                final_file_path = vids_path / filename

            shutil.move(temp_download, final_file_path)
            attachment.size = downloaded_file_size
            attachment.hash = md5sum
            attachment.path = str(final_file_path.relative_to(archive_dir))

        session.commit()

    return


def scraper_test() -> bool:
    return True


def full_test() -> bool:
    pass


def export(session: Session) -> str:
    pass


def main() -> None:
    args = PARSER.parse_args()

    if not args.skip_tests:
        assert scraper_test()

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
            if not args.skip_images or not args.skip_videos:
                update_media(session, dbpath)
        if args.export:
            export(session)
    except:
        LOGGER.exception("Uncaught exception, rolling back db session")
        session.rollback()
        raise
    finally:
        LOGGER.info("Closing db session")
        session.close()


if __name__ == "__main__":
    try:
        main()
    except:
        LOGGER.exception("UNCAUGHT EXCEPTION")
        raise
    finally:
        # ensure connection pool is cleared
        tweetarchiver.TWITTER_SESSION.close()

    # TODO: scrape the profile page for metadata
    # TODO: use account id instead of displayname for identifying accounts
    # TODO: account for possible changes of handle/displayname
    # TODO: save conversation context
    # TODO: save archive to ~/tweetarchiver

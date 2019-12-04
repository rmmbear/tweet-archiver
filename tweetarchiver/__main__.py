import sys
from pathlib import Path

from sqlalchemy.orm import sessionmaker

import tweetarchiver

# getLogger returns logger with different level and config than the one in __init__
# I'm not really sure why that happens
LOGGER = tweetarchiver.LOGGER

def main() -> None:
    # TODO: implement an actual cli using argparse
    args = sys.argv[1:]
    print("Received args:", args)
    if not args or len(args) > 1:
        print("Received wrong number of arguments (expecting one username)")
        while True:
            try:
                args = input(
                    "Enter the name of the account you want to scrape"
                    " (no spaces, no @ symbol) or press CTRL+C to cancel:"
                    ).strip().split()
            except KeyboardInterrupt:
                sys.exit()
            if not args:
                print("No account name given. Try again")
                continue
            if len(args) != 1:
                print("Received more than one argument. Please only enter one account name")
                continue

            break

    username = args[0]
    dbname = f"{username}_twitter_archive.sqlite"
    dbexists = Path(dbname).exists()
    sqla_engine = tweetarchiver.sqla.create_engine(f"sqlite:///{dbname}", echo=False)
    tweetarchiver.DeclarativeBase.metadata.create_all(sqla_engine)
    Session = sessionmaker(bind=sqla_engine)
    LOGGER.info("Creating new db session")
    session = Session()

    oldest = 0
    newest = 0
    scrape_parameters = []
    if dbexists:
        # do not download tweets that already are in db
        newest = tweetarchiver.Tweet.newest_tweet(session)
        oldest = tweetarchiver.Tweet.oldest_tweet(session)
        scrape_parameters.append({"max_id":oldest, "min_id":0}) # only older
        scrape_parameters.append({"max_id":0, "min_id":newest}) # only newer
    else:
        scrape_parameters = [{"max_id":0, "min_id":0}]

    try:
        for kwargs in scrape_parameters:
            for new_tweets in tweetarchiver.scrape_tweets(username, **kwargs):
                session.add_all(new_tweets)
                session.commit()
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
        LOGGER.exception("UNCAUGT EXCEPTION")
        raise

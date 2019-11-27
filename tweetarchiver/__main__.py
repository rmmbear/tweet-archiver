import sys
import time
import logging
import tweetarchiver

LOGGER = logging.getLogger(__name__)
def main():
    args = sys.argv[1:]
    print("Received args:", args)
    if not args or len(args) > 1:
        print("Receivedwrong number of arguments (expecting one username)")
        while True:
            try:
                args = input("Enter the name of the account you want to scrape (no spaces, no @ symbol) or press CTRL+C to cancel:").strip().split()
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
    time_now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(f"./twitter-{username}-scraped-{time_now_str}.csv", mode="w") as id_file:
        tweetarchiver.scrape_tweet_ids(id_file, username)


if __name__ == "__main__":
    try:
        main()
    except:
        LOGGER.error()
        raise

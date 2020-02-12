This is a proof of concept for a twitter profile archiver. It downloads all publicly available tweets from a profile and saves them to sqlite database.

## Dependencies:
- Python 3.7
- requests
- BeautifulSoup4
- sqlalchemy

## How to use this tool:
Clone this repository, ensure you have all required dependencies and from the project's root directory launch it as a module: `python3 -m tweetarchiver username`, where `username` is the account name whose tweets you wish to download.

## Interpreting output:
Tweets are saved to sqlite database file and saved in `~/tweetarchiver/{username}/`, which is also where the attachments are saved. Thirdparty sqlite viewer/editor is currently needed to view archived tweets.

## Caveats:
- This is almost certainly against Twitter's ToS (I'm circumventing the status lookup limit enforced by their API by using the web search)
- Only works for public profiles - locked accounts cannot be archived with this
- likes and retweets are not archived
- Because of issues on Twitter's side, a small number of tweets might be lost. Because exact number of tweets authored by an account is not surfaced (the number shown on profile page includes retweets), it's hard to estimate how big of a problem this actually is. I tested this by archiving the same account three times and then updating each of them with automatic resume function. First and third archives were identical, but the second contained additional tweet at around 28k mark (total tweet count was around 30k) not found in the other two databases.

## TODOs:
- ~~Use a sqlite database instead of csv~~ Done
- ~~Archive images~~ Done
- Create local data and live data parsing tests (live data to be tested before each run, to ensure parser is up-to-date)
- Scrape info from all twitter cards
- Figure out how to archive videos (I could use youtube-dl here)
- Download all tweets in given thread to keep conversation context

This is a proof of concept for a twitter profile archiver. It downloads all publicly available tweets from a profile and saves them to sqlite database.

## Dependencies:
- Python 3.7
- requests
- BeautifulSoup4
- sqlalchemy

## How to use this tool:
Clone this repository, ensure you have all required dependencies and from the project's root directory launch it as a module: `python3 -m tweetarchiver username`, where `username` is the account name whose tweets you wish to download.

## Interpreting output:
Tweets are saved in sqlite database. Its structure is:
table:"account_archive" with columns: tweet_id:string, thread_id:string, timestamp:integer, image_1_url:string, image_2_url:string, image_3_url:string, image_4_url:string, has_video:boolean, text:string

## Caveats:
- This is almost certainly against Twitter's ToS (I'm circumventing the status lookup limit enforced by their API by using the web search)
- Only works for public profiles - locked accounts cannot be archived with this
- likes and retweets are not archived
- Because of issues on Twitter's side, a small number of tweets might be lost. Because exact number of tweets authored by an account is not surfaced (the number shown on profile page includes retweets), it's hard to estimate how big of a problem this actually is. I tested this by archiving the same account three times and then updating each of them with automatic resume function. First and third archives were identical, but the second contained additional tweet at around 28k mark (total tweet count was around 30k) not found in the other two databases.

## TODOs:
- ~~Use a sqlite database instead of csv~~ Done
- Create local data nad live data parsing tests (live data to be tested before each run, to ensure parser is up-to-date)
- After downloading all tweets, also download all the media
- Download all tweets in given thread to keep conversation context
- Figure out how to archive videos (I could use youtube-dl here)

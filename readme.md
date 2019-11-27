This is a proof of concept for a twitter profile archiver. It downloads all publicly available tweets from a profile and saves them to a csv file.

## Dependencies:
- Python 3.6
- requests
- BeautifulSoup4

## How to use this tool:
Clone this repository, ensure you have all required dependencies and from the project's main directory launch it as a module: `python3 -m tweetarchiver <username>`, where <username> is the account name whose tweets you wish to download.

## Caveats:
- This is almost certainly against Twitter's ToS (I'm circumventing the status lookup limit enforced by their API by using the web search)
- Only works for public profiles - locked accounts cannot be archived with this

## TODOs:
- Use a sqlite database instead of csv
- After downloading all tweets, also download all the media
- Figure out how to archive videos (I could use youtube-dl here)

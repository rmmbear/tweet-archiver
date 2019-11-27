import time
import logging
from time import strftime

import requests
import bs4

__VERSION__ = "0.1"
HTML_PARSER = "html.parser"
USER_AGENT = "".join(
    ["TweetRetriever/", __VERSION__,
     "(+https://github.com/rmmbear)"
    ]
)

LOG_FORMAT_TERM = logging.Formatter("[%(levelname)s] %(message)s")
LOG_FORMAT_FILE = logging.Formatter("[%(levelname)s] T+%(relativeCreated)d: %(name)s.%(funcName)s() line:%(lineno)d %(message)s")
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
FH = logging.FileHandler("lastrun.log", mode="w")
FH.setLevel(logging.DEBUG)
FH.setFormatter(LOG_FORMAT_FILE)
CH = logging.StreamHandler()
CH.setLevel(logging.INFO)
CH.setFormatter(LOG_FORMAT_TERM)
#LOGGER.addHandler(CH)
LOGGER.addHandler(FH)

def download(link, max_retries=3):
    """
    """
    retry_count = 0
    #mmm... spagetti
    while True:
        try:
            response = requests.get(
                link, headers={"User-agent":USER_AGENT}, stream=True, timeout=15)
        except requests.exceptions.ReadTimeout:
            print("Connection timed out.")
            if retry_count >= max_retries:
                break

            print(" Retrying ({}/{})".format(retry_count, max_retries))
            continue

        if response.status_code != 200:
            print("Received HTTP error code %s", response.status_code)
            # give up if max_retries has been reached or response is 4xx
            if retry_count >= max_retries or str(response.status_code)[0] == '4':
                break

            retry_count += 1
            print(" Retrying({}/{})".format(retry_count, max_retries))
            continue

        return response.text

    print("COULD NOT COMPLETE DOWNLOAD")
    return ""


def scrape_tweet_ids(id_file, username, since_id=0, max_id=0, page_limit=0, page_delay=1.5):
    """Scrape the  twitter feed using twitter's search to work around
    the API 3.2k status lookup limit.

    1 page = 20 tweets

    since_id = include tweets newer than this id
    max_id = include tweets older than this id
    page_limit = stop after this many pages scraped
    page_delay = delay between consecutive connections in seconds
    """
    query_template = "https://twitter.com/search?f=tweets&vertical=default&q=from:{}"
    query_template = query_template.format(username)
    page_number = 1
    time_last = 0
    while True:
        query_url = query_template
        if since_id:
            query_url = "".join([query_url, " since_id:", str(since_id)])
        if max_id:
            query_url = "".join([query_url, " max_id:", str(max_id)])

        print("Scraping page", page_number, ":", query_url)
        # rate limit to 1 request per page_delay seconds (1.5s by default)
        # use max() as a clamp allowing only 0 and positive values
        time.sleep(max(0, time_last + page_delay - time.time()))
        time.last = time.time()
        results_page = download(query_url)
        parsed_page = bs4.BeautifulSoup(results_page, HTML_PARSER)
        max_id = 0
        for tweet in parsed_page.select(".js-stream-tweet"):
            tweet_id, tweet_line = parse_tweet(tweet)
            id_file.write(tweet_line)
            id_file.write(";\r\n")
            max_id = tweet_id

        #print("max id =", max_id)

        page_number += 1
        if page_limit and page_number > page_limit:
            print("page limit reached (", page_number, ")")
            break

        if not max_id:
            print("could not get max id, breaking")
            break

        # do not include last seen tweet in next search
        max_id = str(int(max_id) - 1)


def parse_tweet(tweet_html):
    line = []
    tweet_id = tweet_html.get("data-tweet-id").strip()
    line.append(tweet_id)
    thread_start = tweet_html.get("data-conversation-id").strip()
    line.append(thread_start)
    for media in tweet_html.select(".js-stream-tweet .AdaptiveMediaOuterContainer"):
        for image in media.select("img"):
            image_url = image.get("src").strip()
            line.append(f"img:{image_url}")
        for video in media.select("video"):
            #TODO: check how youtube-dl does video extraction for twitter
            line.append("video:yes")

    text_container = tweet_html.select(".js-tweet-text")[0]
    text_container_str = str(text_container)

    # here's a little tip from me: don't use anything other than css selectors with bs
    # I wasted about an hour trying to get selecting direct children to work
    # and it was returning the whole container unchanged no matter what I did
    for element in text_container.select("p > *"):
        if element.name == "a":
            if "twitter-atreply" in element.attrs["class"]:
                element_text = element.text
            elif "twitter-hashtag" in element.attrs["class"]:
                element_text = element.text
            elif "twitter-cashtag" in element.attrs["class"]:
                element_text = element.text
            elif "twitter-timeline-link" in element.attrs["class"]:
                if "data-expanded-url" in element.attrs:
                    element_text = element.get("data-expanded-url")
                    if "u-hidden" in element.attrs["class"]:
                        # add padding to avoid smashing into text
                        element_text = f" {element_text}"
                else:
                    element_text = ""
            else:
                print(f"ID={tweet_id} LINK NOT MATCHED")
                LOGGER.error("LINK WAS NOT MATCHED IN ID %S", tweet_id)
                LOGGER.error("%s", element)
        elif element.name == "span":
            if "data-original-codepoint" in element.attrs:
                # as far as I know, this is only done for U+fe0f
                element_text = chr(int(element.get('data-original-codepoint')[2:], 16))
            elif "twitter-hashflag-container" in element.attrs["class"]:
                # this is for promotional hashtags with special "emojis" (they're not actually emojis)
                a = element.find("a")
                element_text = a.text if a else ""
            else:
                print(f"ID={tweet_id} SPAN NOT MATCHED")
                LOGGER.error("SPAN WAS NOT MATCHED IN ID %S", tweet_id)
                LOGGER.error("%s", element)
        elif element.name == "img":
            # this is for emojis - grab the alt text containing actual unicode text
            # and disregard the image
            element_text = element.get("alt")
        else:
            print(f"ID={tweet_id} TAG UNEXPECTED")
            LOGGER.error("TAG WAS UNEXPECTED IN ID %S", tweet_id)
            LOGGER.error("%s", element)

        text_container_str = text_container_str.replace(str(element), element_text, 1)

    text_container = bs4.BeautifulSoup(text_container_str, HTML_PARSER)
    line.append(f"text:{text_container.text}")

    return tweet_id, ",".join(line)

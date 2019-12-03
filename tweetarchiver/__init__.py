import time
import logging
from time import strftime
from typing import Generator
import requests

import sqlalchemy as sqla
from bs4 import BeautifulSoup
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

DeclarativeBase = declarative_base()

__VERSION__ = "0.1"
HTML_PARSER = "html.parser"
USER_AGENT = "".join(
    ["TweetRetriever/", __VERSION__,
     "(+https://github.com/rmmbear)"
    ]
)

LOG_FORMAT_TERM = logging.Formatter("[%(levelname)s] %(message)s")
LOG_FORMAT_FILE = logging.Formatter("[%(levelname)s] T+%(relativeCreated)d: %(name)s.%(funcName)s() line:%(lineno)d %(message)s")
FH = logging.FileHandler("lastrun.log", mode="w")
FH.setLevel(logging.DEBUG)
FH.setFormatter(LOG_FORMAT_FILE)
CH = logging.StreamHandler()
CH.setLevel(logging.DEBUG)
CH.setFormatter(LOG_FORMAT_TERM)
logging.basicConfig(style="%", handlers=(FH, CH))
LOGGER = logging.getLogger("tweetarchiver")


class Tweet(DeclarativeBase):
    """
    """
    __tablename__ = "account_archive"
    tweet_id = sqla.Column(sqla.String, primary_key=True, nullable=False)
    thread_id = sqla.Column(sqla.String, nullable=False)
    timestamp = sqla.Column(sqla.Integer, nullable=False)

    image_1_url = sqla.Column(sqla.String, nullable=True)
    image_2_url = sqla.Column(sqla.String, nullable=True)
    image_3_url = sqla.Column(sqla.String, nullable=True)
    image_4_url = sqla.Column(sqla.String, nullable=True)

    has_video = sqla.Column(sqla.Boolean, nullable=True)

    text = sqla.Column(sqla.String, nullable=True)


    def __init__(self, tweet_html: BeautifulSoup) -> None:
        self.tweet_id = tweet_html.get("data-tweet-id").strip()
        self.thread_id = tweet_html.get("data-conversation-id").strip()
        self.timestamp = tweet_html.select(".js-short-timestamp")[0].get("data-time").strip()

        #self.display_name = tweet_html.get("data-name")
        #self.account_name = tweet_html.get("data-screen-name").strip()
        #self.account_id = tweet_html.get("data-user-id").strip()

        self._parse_media(tweet_html)

        text_container = tweet_html.select(".js-tweet-text")[0]
        text_container_str = str(text_container)

        for element in text_container.select("p > *"):
            if element.name == "a":
                element_text = self._parse_link(element)
            elif element.name == "span":
                if "data-original-codepoint" in element.attrs:
                    # as far as I know, this is only done for U+fe0f
                    element_text = chr(int(element.get('data-original-codepoint')[2:], 16))
                elif "twitter-hashflag-container" in element.attrs["class"]:
                    # this is for promotional hashtags with special "emojis" (they're not actually emojis)
                    a = element.find("a")
                    element_text = a.text if a else ""
                else:
                    print(f"ID={self.tweet_id} SPAN NOT MATCHED")
                    LOGGER.error("SPAN WAS NOT MATCHED IN ID %s", self.tweet_id)
                    LOGGER.error("%s", element)
            elif element.name == "img":
                # this is for emojis - grab the alt text containing actual unicode text
                # and disregard the image
                element_text = element.get("alt")
            else:
                print(f"ID={self.tweet_id} TAG UNEXPECTED")
                LOGGER.error("TAG WAS UNEXPECTED IN ID %s", self.tweet_id)
                LOGGER.error("%s", element)

            text_container_str = text_container_str.replace(str(element), element_text, 1)

        text_container = BeautifulSoup(text_container_str, HTML_PARSER)
        self.text = text_container.text
        if not self.text:
            self.text = None

    def _parse_media(self, tweet_html: BeautifulSoup) -> None:
        images = []
        videos = []

        media_container = tweet_html.select(".js-stream-tweet .AdaptiveMediaOuterContainer", limit=1)
        if not media_container:
            return
        media_container = media_container[0]

        # check if there can be other tags in addition to video and image
        for num, image_element in enumerate(media_container.select("img")):
            image_url = image_element.get("src").strip()
            setattr(self, f"image_{num+1}_url", image_url)
            images.append(image_url)
        for num, video_element in enumerate(media_container.select(".is-video")):
            self.has_video = True
            videos.append("video")
            assert num == 0
            assert video_element

        assert len(images) <= 4
        assert len(videos) <= 1
        assert True if not videos else len(images) == 0


    def _parse_link(self, element: BeautifulSoup) -> str:
        """
        """
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
            elif "data-pre-embedded" in element.attrs and element.attrs["data-pre-embedded"] == "true":
                element_text = ""

            else:
                LOGGER.error("TIMELINE LINK WAS NOT MATCHED IN ID %s", self.tweet_id)

        else:
            print(f"ID={self.tweet_id} LINK NOT MATCHED")
            LOGGER.error("LINK WAS NOT MATCHED IN ID %s", self.tweet_id)
            LOGGER.error("%s", element)

        return element_text


def download(link: str, max_retries: int = 3) -> str:
    """
    """
    retry_count = 0
    while True:
        try:
            response = requests.get(
                link, headers={"User-agent":USER_AGENT}, stream=True, timeout=15)
        except requests.exceptions.ReadTimeout:
            print("Connection timed out.")
            if retry_count >= max_retries:
                break

            print(f" Retrying({retry_count}/{max_retries})")
            continue

        if response.status_code != 200:
            print("Received HTTP error code %s", response.status_code)
            # give up if max_retries has been reached or response is 4xx
            if retry_count >= max_retries or str(response.status_code)[0] == '4':
                break

            retry_count += 1
            print(f" Retrying({retry_count}/{max_retries})")
            continue

        return response.text

    print("COULD NOT COMPLETE DOWNLOAD")
    return ""


def scrape_tweets(username: str, since_id: int = 0, max_id: int = 0,
                  page_limit: int = 0, page_delay: float = 1.5) -> Generator[Tweet, None, None]:
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
    tweets_found = 0
    while True:
        query_url = query_template
        if since_id:
            query_url = "".join([query_url, " since_id:", str(since_id)])
        if max_id:
            query_url = "".join([query_url, " max_id:", str(max_id)])

        print("Scraping page", page_number, ":", query_url)
        # rate limit to 1 request per page_delay seconds
        # use max() as a clamp allowing only 0 and positive values
        time.sleep(max(0, time_last + page_delay - time.time()))
        time.last = time.time()
        results_page = download(query_url)
        results_page = BeautifulSoup(results_page, HTML_PARSER)
        max_id = 0
        new_tweets = []
        for tweet_html in results_page.select(".js-stream-tweet"):
            tweet = Tweet(tweet_html)
            max_id = tweet.tweet_id
            new_tweets.append(tweet)
            tweets_found += 1

        yield new_tweets

        page_number += 1
        if page_limit and page_number > page_limit:
            print(f"Page limit reached ({page_number})")
            break

        if not max_id:
            print("End reached, breaking")
            break

        # do not include last seen tweet in next search
        max_id = str(int(max_id) - 1)

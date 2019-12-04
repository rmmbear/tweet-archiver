import time
import logging
from typing import Generator

import requests
from bs4 import BeautifulSoup
import sqlalchemy as sqla
from sqlalchemy import func as sql_func
from sqlalchemy.orm import exc as sql_exc
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base

DeclarativeBase = declarative_base()

__VERSION__ = "0.1"
HTML_PARSER = "html.parser"
USER_AGENT = "".join(
    ["TweetArchiver/", __VERSION__,
     "(+https://github.com/rmmbear)"
    ]
)

LOG_FORMAT_FILE = logging.Formatter("[%(levelname)s] T+%(relativeCreated)d: %(name)s.%(funcName)s() line:%(lineno)d %(message)s")
LOG_FORMAT_TERM = logging.Formatter("[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("tweetarchiver")
LOGGER.setLevel(logging.DEBUG)
FH = logging.FileHandler("lastrun.log", mode="w")
FH.setLevel(logging.DEBUG)
FH.setFormatter(LOG_FORMAT_FILE)
CH = logging.StreamHandler()
CH.setLevel(logging.INFO)
CH.setFormatter(LOG_FORMAT_TERM)

LOGGER.addHandler(CH)
LOGGER.addHandler(FH)


class Tweet(DeclarativeBase):
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
                    assert False
            elif element.name == "img":
                # this is for emojis - grab the alt text containing actual unicode text
                # and disregard the image
                element_text = element.get("alt")
            else:
                print(f"ID={self.tweet_id} TAG UNEXPECTED")
                LOGGER.error("TAG WAS UNEXPECTED IN ID %s", self.tweet_id)
                LOGGER.error("%s", element)
                assert False

            text_container_str = text_container_str.replace(str(element), element_text, 1)

        text_container = BeautifulSoup(text_container_str, HTML_PARSER)
        self.text = text_container.text
        if not self.text:
            self.text = None


    def _parse_media(self, tweet_html: BeautifulSoup) -> None:
        image_elements = tweet_html.select(".js-stream-tweet .AdaptiveMedia-photoContainer img")
        video_elements = tweet_html.select(".js-stream-tweet .is-video")

        for num, image in enumerate(image_elements):
            image_url = image.get("src").strip()
            setattr(self, f"image_{num+1}_url", image_url)
        if video_elements:
            self.has_video = True

        try:
            assert len(image_elements) <= 4
            assert len(video_elements) <= 1
            assert True if not video_elements else len(image_elements) == 0
        except AssertionError:
            LOGGER.debug("id=%s", self.tweet_id)
            LOGGER.debug("images=%s", image_elements)
            LOGGER.debug("videos=%s", video_elements)
            LOGGER.debug("html=%s", tweet_html)
            raise


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


    @classmethod
    def newest_tweet(cls, session: Session) -> 'Tweet':
        max_timestamp = session.query(sql_func.max(cls.timestamp))
        try:
            tid = session.query(cls).filter(cls.timestamp == max_timestamp).one().tweet_id
            return int(tid)
        except sql_exc.NoResultFound:
            return 0


    @classmethod
    def oldest_tweet(cls, session: Session) -> 'Tweet':
        min_timestamp = session.query(sql_func.min(cls.timestamp))
        try:
            tid = session.query(cls).filter(cls.timestamp == min_timestamp).one().tweet_id
            return int(tid)
        except sql_exc.NoResultFound:
            return 0


def download(link: str, session: requests.Session, max_retries: int = 3) -> str:
    """Download content at specified url, return response's text."""
    retry_count = 0
    query = requests.Request("GET", link, headers={"User-agent":USER_AGENT})
    query = query.prepare()
    while True:
        try:
            response = session.send(query, stream=True, timeout=15)
            response.raise_for_status()
            return response.text
        except requests.HTTPError:
            LOGGER.error("Received HTTP error code %s", response.status_code)
        except requests.Timeout:
            LOGGER.error("Connection timed out")
        except requests.ConnectionError:
            LOGGER.error("Could not establish a new connection")
            #most likely a client-side connection error, do not retry
            retry_count = max_retries
        except requests.RequestException:
            LOGGER.error("Unexpected request exception")
            LOGGER.debug("request url = %s", query.url)
            LOGGER.debug("request method %s", query.method)
            LOGGER.debug("request headers %s", query.headers)
            LOGGER.debug("request body = %s", query.body)
            #ambiguous error, do not retry
            retry_count = max_retries

        if retry_count >= max_retries:
            break

        retry_count += 1
        print(f" Retrying({retry_count}/{max_retries})")

    print("COULD NOT COMPLETE DOWNLOAD")
    return ""


def scrape_tweets(username: str, min_id: int = 0, max_id: int = 0,
                  page_limit: int = 0, page_delay: float = 1.5) -> Generator[Tweet, None, None]:
    """Scrape an account's twitter feed using twitter's search to work around
    their API's 3.2k status lookup limit.

    1 page = 20 tweets

    min_id = include tweets newer than this id
    max_id = include tweets older than this id
    page_limit = stop after this many pages scraped
    page_delay = delay between consecutive connections in seconds

    min_id and max_id should be ids of existing tweets. This function
    automatically decrements/increments them to exclude original idsfrom
    results.
    """
    query_template = "https://twitter.com/search?f=tweets&vertical=default&q=from:{}"
    query_template = query_template.format(username)

    # make sure these ids are not returned by our query
    if min_id:
        min_id += 1
    if max_id:
        max_id -= 1

    loop_start = 0
    start_time = time.time()
    page_number = 1
    tweets_found = 0
    with requests.Session() as req_session:
        while True:
            query_url = query_template
            if min_id:
                query_url = f"{query_url} since_id:{min_id}"
            if max_id:
                query_url = f"{query_url} max_id:{max_id}"

            print("Scraping page", page_number, ":", query_url)
            # rate limit to 1 request per page_delay seconds
            time.sleep(max(0, loop_start + page_delay - time.time()))
            results_page = download(query_url, req_session)
            loop_start = time.time()
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

    # TODO: remove clutter
    time_spent_s = (time.time() - start_time)
    time_spent_m = time_spent_s // 60
    time_spent_s = time_spent_s % 60
    time_spent_h = time_spent_m // 60
    time_spent_m = time_spent_m % 60
    time_str = f"{time_spent_h:.0f}:{time_spent_m:.0f}:{time_spent_s:.0f}"
    LOGGER.info("Found %s new tweets", tweets_found)
    LOGGER.info("This took %s", time_str)

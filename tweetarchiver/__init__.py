import time
from calendar import timegm
import json
import logging
from hashlib import md5
from typing import Generator, BinaryIO, Optional, List, Tuple, Union

import requests
import sqlalchemy as sqla
from sqlalchemy import func as sql_func
from sqlalchemy.orm import exc as sql_exc
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base
from bs4 import BeautifulSoup

DeclarativeBase = declarative_base()

__VERSION__ = "0.1"

LOG_FORMAT_FILE = logging.Formatter("[%(levelname)s] %(asctime)s: %(name)s.%(funcName)s() line:%(lineno)d %(message)s")
LOG_FORMAT_TERM = logging.Formatter("[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("tweetarchiver")
LOGGER.setLevel(logging.DEBUG)
FH = logging.FileHandler("lastrun.log", mode="w")
FH.setLevel(logging.DEBUG)
FH.setFormatter(LOG_FORMAT_FILE)
TH = logging.StreamHandler()
TH.setLevel(logging.DEBUG)
TH.setFormatter(LOG_FORMAT_TERM)

LOGGER.addHandler(TH)
LOGGER.addHandler(FH)

HTML_PARSER = "html.parser"
USER_AGENT = "".join(
    ["TweetArchiver/", __VERSION__,
     "(+https://github.com/rmmbear/tweet-archiver)"
    ]
)

# Note that session should be closed by the batch functions which benefit from
# connection pooling (scrape_tweets, for example)
# it should also clear automatically in case of uncaught exception if module was
# called from __main__.main()
TWITTER_SESSION = requests.Session()
TWITTER_SESSION.headers["User-Agent"] = USER_AGENT
TWITTER_SESSION.headers["Accept-Language"] = "en-US,en;q=0.5"
TWITTER_SESSION.headers["x-twitter-client-language"] = "en"
#TWITTER_SESSION.headers["Accept-Encoding"] = "gzip, deflate"
#TWITTER_SESSION.headers["Accept"] = "gzip, deflate"
#TWITTER_SESSION.headers["Connection"] = "keep-alive"


def set_guest_token() -> None:
    """Set the authorization and guest token in twitter
    session's headers. This is only necessary for videos and polls, all
    other parts of the site can be accessed without any authorization.
    """
    TWITTER_SESSION.headers["Authorization"] = "Bearer AAAAAAAAAAAAAAAAAAAAAPYXBAAAAAAACLXUNDekMxqa8h%2F40K4moUkGsoc%3DTYfbDKbT3jJPCEVnMYqilB28NHfOPqkca3qaAxGfsyKCs0wRbw"
    link = "https://api.twitter.com/1.1/guest/activate.json"
    response_json = json.loads(download(link, method="POST"))
    guest_token = None
    try:
        guest_token = response_json["guest_token"]
    except:
        LOGGER.error("Did not receive guest token ")
        LOGGER.error("Contents of response: \n %s", json.dumps(response_json, indent=4))
        raise RuntimeError("Could not retrieve twitter guest token")

    LOGGER.debug("setting guest token to %s", guest_token)
    TWITTER_SESSION.headers["x-guest-token"] = guest_token


def download(link: str,
             return_response: bool = False,
             to_file: Optional[BinaryIO] = None,
             method: str = "GET",
             headers: Optional[dict] = None,
             allow_redirects: bool = True,
             max_retries: int = 3) -> Union[str, requests.Response]:
    """

    If to_file is not None, write response to it and return its md5 hash.
    """
    #TODO: consider splitting this function - one returning response objects, the other strings
    exp_delay = [2**x for x in range(max_retries)]
    retry_count = 0
    query = requests.Request(method, link)
    query = TWITTER_SESSION.prepare_request(query)
    if headers:
        query.headers.update(headers)
    while True:
        try:
            response = TWITTER_SESSION.send(query, allow_redirects=allow_redirects, stream=True, timeout=15)
            response.raise_for_status()
            if return_response:
                return response

            if to_file:
                md5_hash = md5()
                for chunk in response.iter_content(chunk_size=(1024**2)*3):
                    to_file.write(chunk)
                    md5_hash.update(chunk)
                return md5_hash.hexdigest()

            return response.text
        except requests.HTTPError:
            LOGGER.error("Received HTTP error code %s", response.status_code)
        except requests.Timeout:
            LOGGER.error("Connection timed out")
        except requests.ConnectionError:
            LOGGER.error("Could not establish a new connection")
            #most likely a client-side connection error, do not retry
            retry_count = max_retries
        except requests.RequestException as err:
            LOGGER.error("Unexpected request exception")
            LOGGER.error("request url = %s", query.url)
            LOGGER.error("request method = %s", query.method)
            LOGGER.error("request headers = %s", query.headers)
            LOGGER.error("request body = %s", query.body)
            raise err

        if retry_count >= max_retries:
            break

        retry_count += 1
        delay = exp_delay[retry_count-1]
        print(f" Retrying({retry_count}/{max_retries}) in {delay}s")
        time.sleep(delay)

    print("COULD NOT COMPLETE DOWNLOAD")
    return ""


class TweetHTML(DeclarativeBase):
    """Table storing tweets in html form. For testing purposes only.
    """
    __tablename__ = "account_html"
    tweet_id = sqla.Column(sqla.Integer, primary_key=True, nullable=False)
    html = sqla.Column(sqla.String, nullable=False)
    scraped_on = sqla.Column(sqla.Integer, nullable=False)

    def parse(self) -> "Tweet":
        return Tweet(BeautifulSoup(self.html, HTML_PARSER).select(".js-stream-tweet")[0])


    def __init__(self, tweet_html: BeautifulSoup, timestamp: int) -> None:
        self.tweet_id = tweet_html.get("data-tweet-id").strip()
        self.html = str(tweet_html)
        self.scraped_on = timestamp


    @classmethod
    def newest_tweet(cls, session: Session) -> int:
        max_id = session.query(sql_func.max(cls.tweet_id))
        try:
            tid = session.query(cls).filter(cls.tweet_id == max_id).one().tweet_id
            return int(tid)
        except sql_exc.NoResultFound:
            return 0


    @classmethod
    def oldest_tweet(cls, session: Session) -> int:
        min_id = session.query(sql_func.min(cls.tweet_id))
        try:
            tid = session.query(cls).filter(cls.tweet_id == min_id).one().tweet_id
            return int(tid)
        except sql_exc.NoResultFound:
            return 0


class Attachment(DeclarativeBase):
    __tablename__ = "account_attachments"
    id = sqla.Column(sqla.Integer, primary_key=True)
    url = sqla.Column(sqla.String, nullable=False, primary_key=True)
    tweet_id = sqla.Column(sqla.Integer, sqla.ForeignKey("account_archive.tweet_id"), nullable=False)
    position = sqla.Column(sqla.Integer, nullable=False)
    sensitive = sqla.Column(sqla.Boolean, nullable=False)
    type = sqla.Column(sqla.String, nullable=True)
    size = sqla.Column(sqla.Integer, nullable=True)
    hash = sqla.Column(sqla.String, nullable=True)
    path = sqla.Column(sqla.String, nullable=True)

    #attached = relationship(Tweet, back_populates="media")

    @classmethod
    def from_html(cls, tweet_html: BeautifulSoup) -> List["Attachment"]:
        tweet_id = int(tweet_html.get("data-tweet-id").strip())
        video_elements = tweet_html.select(".js-stream-tweet .is-video")
        image_elements = tweet_html.select(".js-stream-tweet .AdaptiveMedia-photoContainer img")
        tombstone_label = tweet_html.select("AdaptiveMediaOuterContainer .Tombstone-label").text
        sensitive = "media may contain sensitive material" in tombstone_label

        media = []
        for num, image in enumerate(image_elements):
            image_url = image.get("src").strip()
            #setattr(self, f"image_{num+1}_url", image_url)
            media.append(
                cls(
                    url=image_url,
                    tweet_id=tweet_id,
                    position=num+1,
                    sensitive=sensitive,
                    type=f"img:{image_url.rsplit('.', maxsplit=1)[-1]}"
                    )
                )
        if video_elements:
            vid_url = tweet_html.select(".PlayableMedia-player")[0].get("style").split(";") # maybe use regex
            # 'gifs' (actually short mp4s) can be downloaded directly
            # for actual vids m3u fuckery is needed
            # relevant snippet of the gif being embedded
            """
            <div class="AdaptiveMediaOuterContainer">
            <div class="AdaptiveMedia is-video has-autoplayable-media">
            <div class="AdaptiveMedia-container">
            <div class="AdaptiveMedia-video">
            <div class="AdaptiveMedia-videoContainer">
            <div class="PlayableMedia PlayableMedia--gif">
            <div class="PlayableMedia-container">
            <div class="PlayableMedia-player" data-border-bottom-left-radius="" data-border-bottom-right-radius="" data-border-top-left-radius="" data-border-top-right-radius="" data-playable-media-url="" data-use-b-version-of-react-player="" data-use-player-precache="" data-use-react-player="" style="padding-bottom: 100.0%; background-image:url('https://pbs.twimg.com/tweet_video_thumb/DYhrrxGV4AU5_-9.jpg')">
            """
            video = cls(
                #url=f"https://twitter.com/user/status/{tweet_id}",
                tweet_id=tweet_id,
                position=1,
                sensitive=sensitive)
            media.append(video)

        try:
            assert len(image_elements) <= 4
            assert len(video_elements) <= 1
            assert True if not video_elements else len(image_elements) == 0
        except AssertionError:
            LOGGER.debug("id=%s", tweet_id)
            LOGGER.debug("images=%s", image_elements)
            LOGGER.debug("videos=%s", video_elements)
            LOGGER.debug("html=%s", tweet_html)
            raise

        return media


class Account(DeclarativeBase):
    __tablename__ = "account_details"
    account_id = sqla.Column(sqla.Integer, primary_key=True)
    join_date = sqla.Column(sqla.Integer)

    name = sqla.Column(sqla.String)
    handle = sqla.Column(sqla.String)
    link = sqla.Column(sqla.String)
    description = sqla.Column(sqla.String)
    avatar = sqla.Column(sqla.String)
    location = sqla.Column(sqla.String)

    previous_names = sqla.Column(sqla.String)
    previous_handles = sqla.Column(sqla.String)
    previous_links = sqla.Column(sqla.String)
    previous_descriptions = sqla.Column(sqla.String)
    previous_avatars = sqla.Column(sqla.String)
    previous_locations = sqla.Column(sqla.String)


class Tweet(DeclarativeBase):
    __tablename__ = "account_archive"
    tweet_id = sqla.Column(sqla.Integer, primary_key=True, nullable=False)
    thread_id = sqla.Column(sqla.Integer, nullable=False)
    timestamp = sqla.Column(sqla.Integer, nullable=False)
    account_id = sqla.Column(sqla.Integer, sqla.ForeignKey("account_details.account_id"), nullable=False)

    replying_to = sqla.Column(sqla.Integer, nullable=True)
    qrt_id = sqla.Column(sqla.Integer, nullable=True)

    tags = sqla.Column(sqla.JSON, nullable=True)#includes info on type of tags and their positions in tweet
    poll_data = sqla.Column(sqla.JSON, nullable=True)#
    # total_votes = int
    # choices = [{prompt=str, percentage=int, }]
    #
    poll_finished = sqla.Column(sqla.Boolean, nullable=True)

    has_video = sqla.Column(sqla.Boolean, nullable=False)
    image_count = sqla.Column(sqla.Integer, nullable=False)
    replies = sqla.Column(sqla.Integer, nullable=False)
    retweets = sqla.Column(sqla.Integer, nullable=False)
    favorites = sqla.Column(sqla.Integer, nullable=False)

    embedded_link = sqla.Column(sqla.String, nullable=True)
    text = sqla.Column(sqla.String, nullable=True)

    media = relationship(Attachment, order_by=Attachment.position)


    def __init__(self, tweet_html: BeautifulSoup) -> None:
        self.tweet_id = int(tweet_html.get("data-tweet-id").strip())
        self.thread_id = int(tweet_html.get("data-conversation-id").strip())
        self.timestamp = int(tweet_html.select_one(".js-short-timestamp").get("data-time").strip())
        self.account_id = int(tweet_html.get("data-user-id").strip())

        self.replying_to = None # need a second pass on specific threads to get reply chains
        qrt = tweet_html.select_one(".QuoteTweet-innerContainer")
        self.qrt_id = qrt.get("data-item-id").strip() if qrt else None

        self.tags = None
        poll_data, poll_finished = self._get_poll_data(tweet_html)
        self.poll_data = poll_data
        self.poll_finished = poll_finished

        self.has_video = bool(tweet_html.select(".js-stream-tweet .is-video"))
        self.image_count = len(tweet_html.select(".js-stream-tweet .AdaptiveMedia-photoContainer img"))
        replies = tweet_html.select_one(".ProfileTweet-action--reply .ProfileTweet-actionCount").get("data-tweet-stat-count")
        retweets = tweet_html.select_one(".ProfileTweet-action--retweet .ProfileTweet-actionCount").get("data-tweet-stat-count")
        favorites = tweet_html.select_one(".ProfileTweet-action--favorite .ProfileTweet-actionCount").get("data-tweet-stat-count")
        self.favorites = int(favorites)
        self.retweets = int(retweets)
        self.replies = int(replies)

        self.links = []
        self.embedded_link = self._get_embedded_link(tweet_html)
        self.text = self._get_tweet_text(tweet_html)

        #if not self.embedded_link and self.links and not self.image_count:
        #    LOGGER.debug("Using last link in post as an embed link in tweet %s", self.tweet_id)
        #    self.embedded_link = self.links[-1]

    def _get_tweet_text(self, tweet_html: BeautifulSoup) -> Optional[str]:
        text_container = tweet_html.select_one(".js-tweet-text")
        text_container_str = str(text_container)

        for element in text_container.select("p > *"):
            if element.name == "a":
                element_text = self._untangle_link(element)
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
                # this is for emojis - grab the alt text containing actual unicode point
                # and disregard the image
                element_text = element.get("alt")
            else:
                print(f"ID={self.tweet_id} TAG UNEXPECTED")
                LOGGER.error("TAG WAS UNEXPECTED IN ID %s", self.tweet_id)
                LOGGER.error("%s", element)
                assert False

            text_container_str = text_container_str.replace(str(element), element_text, 1)

        text_container = BeautifulSoup(text_container_str, HTML_PARSER)
        text = text_container.text
        if not text:
            text = None

        return text


    def _untangle_link(self, element: BeautifulSoup) -> str:
        """
        """
        #hashtags
        if "twitter-atreply" in element.attrs["class"]:
            element_text = element.text
        elif "twitter-hashtag" in element.attrs["class"]:
            element_text = element.text
        elif "twitter-cashtag" in element.attrs["class"]:
            element_text = element.text
        elif "twitter-timeline-link" in element.attrs["class"]:
            if "data-expanded-url" in element.attrs:
                element_text = element.get("data-expanded-url")
                self.links.append(element_text)
                if "u-hidden" in element.attrs["class"]:
                    # link is displayed as a twitter card only, do not add it to text
                    LOGGER.debug("card link = %s", self.embedded_link)
                    LOGGER.debug("hidden link = %s", element_text)
                    if self.embedded_link:
                        assert element_text == self.embedded_link
                    else:
                        # this happens for youtube links
                        self.embedded_link = element_text
                    element_text = ""
            elif "data-pre-embedded" in element.attrs and element.attrs["data-pre-embedded"] == "true":
                # pic.twitter.com links, i.e. link to the embedded attachments
                # possibly legacy or meant for platforms where pictures were not displayed automatically?
                element_text = ""
            else:
                LOGGER.error("TIMELINE LINK WAS NOT MATCHED IN ID %s", self.tweet_id)
                raise RuntimeError()
        else:
            print(f"ID={self.tweet_id} LINK NOT MATCHED")
            LOGGER.error("LINK WAS NOT MATCHED IN ID %s", self.tweet_id)
            LOGGER.error("%s", element)
            raise RuntimeError()


        return element_text


    def _get_embedded_link(self, tweet_html: BeautifulSoup) -> Optional[str]:
        #FIXME: youtube links in older tweets (from 2014 and before) are not picked up
        # they are technically shown as embeds on web twitter, but they're not
        # included as timeline links in the tweet
        card_container = tweet_html.select_one(".card2.js-media-container")
        if not card_container:
            return None

        card_name = card_container.get("data-card2-name")
        if card_name.startswith("poll"):
            # _get_poll_data already took care of this
            return None

        frame_container = card_container.select_one("div")
        #LOGGER.debug(frame_container)
        frame_url = frame_container.get("data-src")
        frame_url = f"https://twitter.com{frame_url}"

        LOGGER.debug("Downloading card frame from tweet %s", self.tweet_id)
        # authorization in form of referer header is required, otherwise 403 is returned
        frame = download(frame_url, headers={"Referer":f"https://twitter.com/user/status/{self.tweet_id}"})
        frame = BeautifulSoup(frame, HTML_PARSER)
        #LOGGER.debug(frame)
        embedded_link = frame.select_one(".TwitterCard .TwitterCard-container").get("href")
        if not embedded_link:
            if card_name == "player":
                LOGGER.debug("Expected failure - did not find embedded link for 'player' card in tweet %s", self.tweet_id)
                return None

            LOGGER.error("Could not find embedded link for card '%s' in tweet %s", card_name, self.tweet_id)
            raise RuntimeError()

        if embedded_link.startswith("http:"):
            embedded_link = f"{'https'}{embedded_link[4:]}"
            # avoid unnecessary redirects for links generated before t.co started fully encrypting traffic

        link_query = download(embedded_link, method="HEAD", return_response=True, allow_redirects=False)
        if link_query.is_redirect:
            LOGGER.debug("Detected redirect from '%s' to '%s'", embedded_link, link_query.headers["location"])
            embedded_link = link_query.headers["location"]
        LOGGER.debug("Card type: %s, Card link: %s", card_name, embedded_link)
        return embedded_link


    def _get_poll_data(self, tweet_html: BeautifulSoup) -> Tuple[Optional[dict], Optional[bool]]:
        poll_object = {}
        card_container = tweet_html.select_one(".card2.js-media-container")

        if not card_container:
            return None, None

        card_name = card_container.get("data-card2-name")
        if not card_name.startswith("poll"):
            return None, None

        poll_frame_container = card_container.select_one("div")
        frame_url = poll_frame_container.get("data-src")
        frame_url = f"https://twitter.com{frame_url}"

        LOGGER.debug("Downloading poll frame from tweet %s", self.tweet_id)
        # authorization in form of referer header is required, otherwise 403 is returned
        poll_frame = download(frame_url, headers={"Referer":f"https://twitter.com/user/status/{self.tweet_id}"})
        poll_frame = BeautifulSoup(poll_frame, HTML_PARSER)

        card_serialized = poll_frame.select_one("[type=\"text/twitter-cards-serialization\"]").text
        card_serialized = json.loads(card_serialized)["card"]
        poll_object["is_open"] = card_serialized["is_open"]
        if isinstance(poll_object["is_open"], str):
            poll_object["is_open"] = {"false":False, "true":True}[poll_object["is_open"].lower()]

        poll_object["choice_count"] = card_serialized["choice_count"]
        poll_object["end_time"] = timegm(time.strptime(card_serialized["end_time"], "%Y-%m-%dT%H:%M:%S%z"))
        # ^ store time as unix timestamp for consistency

        poll_container = poll_frame.select_one(".TwitterCard .CardContent .PollXChoice")
        poll_object["votes_total"] = poll_container.get("data-poll-init-state")
        poll_object["winning_index"] = poll_container.get("data-poll-vote-majority")
        #poll_object["voted_for_index"] = poll_container.get("data-poll-user-choice")
        poll_choices = poll_container.select(".PollXChoice-choice .PollXChoice-choice--text")

        poll_object["choices"] = []
        for choice_num in range(poll_object["choice_count"]):
            choice_html = poll_choices[choice_num]
            choice = dict()
            choice["votes"] = card_serialized[f"count{choice_num+1}"]
            choice["votes_percent"] = choice_html.select_one(".PollXChoice-progress").text
            choice["label"] = choice_html.select_one("span:nth-of-type(2)").text
            poll_object["choices"].append(choice)

        assert len(poll_object["choices"]) == poll_object["choice_count"]
        #LOGGER.debug("%s", poll_object)
        #LOGGER.debug("%s", card_serialized)
        return poll_object, poll_object["is_open"]



    @classmethod
    def newest_tweet(cls, session: Session) -> int:
        max_id = session.query(sql_func.max(cls.tweet_id))
        try:
            tid = session.query(cls).filter(cls.tweet_id == max_id).one().tweet_id
            return int(tid)
        except sql_exc.NoResultFound:
            return 0


    @classmethod
    def oldest_tweet(cls, session: Session) -> int:
        min_id = session.query(sql_func.min(cls.tweet_id))
        try:
            tid = session.query(cls).filter(cls.tweet_id == min_id).one().tweet_id
            return int(tid)
        except sql_exc.NoResultFound:
            return 0


def scrape_video() -> None:
    """Return list of video rows
    """
    ...

def scrape_images() -> None:
    """Return list of image rows
    """
    ...


def scrape_tweets(username: str, min_id: int = 0, max_id: int = 0,
                  page_limit: int = 0, page_delay: float = 1.5
                 ) -> Generator[List[BeautifulSoup], None, None]:
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

    Return generator yielding BeautifulSoup parsed html.
    """
    query_template = "https://twitter.com/search?f=tweets&vertical=default&q=from:{}"
    query_template = query_template.format(username)

    # make sure these ids are not returned by our query
    if min_id:
        min_id += 1
    if max_id:
        max_id -= 1

    loop_start = 0.0
    page_number = 1
    tweets_found = 0
    while True:
        query_url = query_template
        if min_id:
            query_url = f"{query_url} since_id:{min_id}"
        if max_id:
            query_url = f"{query_url} max_id:{max_id}"

        print("Scraping page", page_number, ":", query_url)
        # rate limit to 1 request per page_delay seconds
        time.sleep(max(0, loop_start + page_delay - time.time()))
        results_page = download(query_url)
        loop_start = time.time()
        results_page = BeautifulSoup(results_page, HTML_PARSER)
        max_id = 0
        new_tweets = []
        for tweet_html in results_page.select(".js-stream-tweet"):
            # it is theoretically possible for temporarily suspended accounts
            # to still show up in search results just like regular tweets do
            # but containing no actual content apart from suspension notice.
            # When encountered, scraping must be stopped immediately
            # TODO: detect suspended accounts
            max_id = tweet_html.get("data-tweet-id").strip()
            new_tweets.append(tweet_html)
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
        max_id = int(max_id) - 1

    TWITTER_SESSION.close()

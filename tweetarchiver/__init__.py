import time
import json
import logging
from hashlib import md5
from calendar import timegm
from urllib.parse import urlparse
from typing import Generator, BinaryIO, Optional, List, Tuple, Union, NamedTuple

import requests
import sqlalchemy as sqla
from sqlalchemy import func as sql_func
from sqlalchemy.orm import exc as sql_exc
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base
from bs4 import BeautifulSoup

DeclarativeBase = declarative_base()

__VERSION__ = "0.1"

LOG_FORMAT_TERM = logging.Formatter("[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("tweetarchiver")
LOGGER.setLevel(logging.DEBUG)
TH = logging.StreamHandler()
TH.setLevel(logging.INFO)
TH.setFormatter(LOG_FORMAT_TERM)

LOGGER.addHandler(TH)


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


#https://twitter.com/intent/user?user_id=XXX
#

def set_guest_token() -> None:
    """Set the authorization and guest token in twitter
    session's headers. This is only necessary for videos, all
    other parts of the site can be accessed without any authorization.
    """
    TWITTER_SESSION.headers["Authorization"] = "Bearer AAAAAAAAAAAAAAAAAAAAAPYXBAAAAAAACLXUNDekMxqa8h%2F40K4moUkGsoc%3DTYfbDKbT3jJPCEVnMYqilB28NHfOPqkca3qaAxGfsyKCs0wRbw"
    link = "https://api.twitter.com/1.1/guest/activate.json"
    response_json = download(link, method="POST").response.text
    response_json = json.loads(response_json)
    guest_token = None
    try:
        guest_token = response_json["guest_token"]
    except:
        LOGGER.error("Did not receive guest token ")
        LOGGER.error("Contents of response: \n %s", json.dumps(response_json, indent=4))
        raise RuntimeError("Could not retrieve twitter guest token")

    LOGGER.debug("setting guest token to %s", guest_token)
    TWITTER_SESSION.headers["x-guest-token"] = guest_token


class Response(NamedTuple):
    """Convenient """
    response: requests.Response
    size: int = 0
    hash: str = ""


def download(link: str,
             method: str = "GET",
             to_file: Optional[BinaryIO] = None,
             headers: Optional[dict] = None,
             allow_redirects: bool = True,
             max_retries: int = 3) -> "Response":
    """
    Return Response named tuple
        Response.response - requests.Response object
        Response.size     - size of downloaded file, 0 if to_file is None
        Response.hash     - md5 hash of the downloaded file, empty string if to_file is None
    """
    exp_delay = [2**(x+1) for x in range(max_retries)]
    retry_count = 0
    query = requests.Request(method, link)
    query = TWITTER_SESSION.prepare_request(query)
    LOGGER.debug("Making %s request to %s", method, link)
    if headers:
        query.headers.update(headers)
    while True:
        try:
            response = TWITTER_SESSION.send(query, allow_redirects=allow_redirects, stream=True, timeout=15)
            response.raise_for_status()

            if to_file:
                size = 0
                md5_hash = md5()
                for chunk in response.iter_content(chunk_size=(1024**2)*3):
                    to_file.write(chunk)
                    md5_hash.update(chunk)
                    size += len(chunk)

                #LOGGER.info("left=%s right=%s", size, response.headers["content-length"])
                assert size == int(response.headers["content-length"])
                return Response(response=response, size=size, hash=md5_hash.hexdigest())

            return Response(response)
        except requests.HTTPError:
            LOGGER.error("Received HTTP error code %s", response.status_code)
            if response.status_code in [404] or retry_count >= max_retries:
                raise
        except requests.Timeout:
            LOGGER.error("Connection timed out")
            if retry_count >= max_retries:
                raise
        except requests.ConnectionError:
            LOGGER.error("Could not establish a new connection")
            #most likely a client-side connection error, do not retry
            raise
        except requests.RequestException as err:
            LOGGER.error("Unexpected request exception")
            LOGGER.error("request url = %s", query.url)
            LOGGER.error("request method = %s", query.method)
            LOGGER.error("request headers = %s", query.headers)
            LOGGER.error("request body = %s", query.body)
            raise err

        retry_count += 1
        delay = exp_delay[retry_count-1]
        print(f"Retrying ({retry_count}/{max_retries}) in {delay}s")
        LOGGER.error("Retrying (%s/%s) in %ss", retry_count, max_retries, delay)
        time.sleep(delay)


class TweetHTML(DeclarativeBase):
    """Table storing tweets in html form. For testing purposes only.
    """
    __tablename__ = "account_html"
    tweet_id = sqla.Column(sqla.Integer, primary_key=True, nullable=False)
    html = sqla.Column(sqla.String, nullable=False)
    scraped_on = sqla.Column(sqla.Integer, nullable=False)


    def parse(self) -> "Tweet":
        return Tweet.from_html(BeautifulSoup(self.html, HTML_PARSER).select_one(".js-stream-tweet"))


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
    url = sqla.Column(sqla.String, nullable=False)
    # while this is not the case 90% of the time, urls can repeat
    tweet_id = sqla.Column(sqla.Integer, sqla.ForeignKey("account_archive.tweet_id"), nullable=False)
    position = sqla.Column(sqla.Integer, nullable=False) # to retain order in which images are displayed
    sensitive = sqla.Column(sqla.Boolean, nullable=False)

    type = sqla.Column(sqla.String, nullable=False)
    size = sqla.Column(sqla.Integer, nullable=True)
    hash = sqla.Column(sqla.String, nullable=True)
    path = sqla.Column(sqla.String, nullable=True)

    attached = relationship("Tweet", back_populates="media")

    @classmethod
    def from_html(cls, tweet_html: BeautifulSoup) -> List["Attachment"]:
        tweet_id = int(tweet_html.get("data-tweet-id").strip())
        video_elements = tweet_html.select(".js-stream-tweet .is-video")
        image_elements = tweet_html.select(".js-stream-tweet .AdaptiveMedia-photoContainer img")
        tombstone_label = tweet_html.select_one(".AdaptiveMediaOuterContainer .Tombstone-label")
        sensitive = False
        if tombstone_label:
            tombstone_label = tombstone_label.text
            sensitive = "media may contain sensitive material" in tombstone_label

        media = []
        for num, image in enumerate(image_elements):
            image_url = image.get("src").strip()
            #TODO: detect apngs
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
            gif = tweet_html.select_one(".PlayableMedia--gif")
            if gif:
                video_type = "vid:gif"
                # 'gifs' (actually short mp4s) can be downloaded directly, for actual vids m3u fuckery is needed
                # note that in web twitter the furthest descendant of .PlayableMedia-player
                # would be a video tag containing the direct url to the video
                # but because of the approach for accessing twitter, we do not have accesss to that
                # video tag als ocontains url to a 'poster' displayed while the video is not playing
                # image is hosted at https://pbs.twimg.com/tweet_video_thumb/{file}
                # and the video at https://video.twimg.com/tweet_video/{file}
                # the poster image and video file always use the same name, so if we know that the
                # image is named EOFhYRnWkAIlIK8.jpg then the url for our video
                # is https://video.twimg.com/tweet_video/EOFhYRnWkAIlIK8.mp4
                # as it happens, the .PlayableMedia-player element contains a style attribute, which
                # includes a background image - this is the exact same file as in the video tag
                # this means we can:
                # 1 grab .PlayableMedia-playerelement
                # 2 get its style attribute
                # 3 parse it and get the image url
                # 4 place the filename in video url template
                # and we have the url to the video
                player_style = tweet_html.select_one(".PlayableMedia-player").get("style")
                player_style = dict([x.strip().split(":", maxsplit=1) for x in player_style.split(";")])
                assert player_style["background-image"].startswith("url")

                image_url = player_style["background-image"][5:-2:]
                image_url = urlparse(image_url)
                # take path -> split on elements, take the last one -> split on extension, take name
                video_name = image_url.path.rsplit("/", maxsplit=1)[-1].rsplit(".", maxsplit=1)[0]
                vid_url = f"https://video.twimg.com/tweet_video/{video_name}.mp4"

            else:
                video_type = "vid:mp4"
                vid_url = f"https://twitter.com/user/status/{tweet_id}"

            video = cls(
                url=vid_url,
                tweet_id=tweet_id,
                position=1,
                sensitive=sensitive,
                type=video_type)
            media.append(video)

        return media


    @classmethod
    def with_missing_files(cls, session: Session) -> List["Attachment"]:
        attachments_missing_files = session.query(cls).filter(cls.path == None).order_by(cls.tweet_id)
        return attachments_missing_files.all()


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

    poll_data = sqla.Column(sqla.JSON, nullable=True)
    poll_finished = sqla.Column(sqla.Boolean, nullable=True) # if false, will need to be updated

    has_video = sqla.Column(sqla.Boolean, nullable=False)
    image_count = sqla.Column(sqla.Integer, nullable=False)
    replies = sqla.Column(sqla.Integer, nullable=False)
    retweets = sqla.Column(sqla.Integer, nullable=False)
    favorites = sqla.Column(sqla.Integer, nullable=False)

    embedded_link = sqla.Column(sqla.String, nullable=True)
    text = sqla.Column(sqla.String, nullable=True)
    poi = sqla.Column(sqla.String, nullable=True) # format is "{label}:{place_id}"
    # author can choose to include label location to the tweet when composing it
    # this is different from the location added automatically to tweets if location data is enabled
    # I'm deciding to keep this only because it has to be included manually at which point it becomes
    #                                            content
    withheld_in = sqla.Column(sqla.String, nullable=True)
    # two types of values possible: "unknown" if tweet is withheld but where exactly is not known
    # otherwise two letter country identifiers (ISO 3166-1 alpha-2) separated with commas

    media = relationship(Attachment, order_by=Attachment.position)

    @classmethod
    def from_html(cls, tweet_html: BeautifulSoup) -> "Tweet":
        new_tweet = cls()
        new_tweet.tweet_id = int(tweet_html.get("data-tweet-id").strip())
        new_tweet.thread_id = int(tweet_html.get("data-conversation-id").strip())
        new_tweet.account_id = int(tweet_html.get("data-user-id").strip())

        withheld = tweet_html.select_one(".StreamItemContent--withheld")

        if withheld:
            LOGGER.error("Encountered a withheld tweet %s", new_tweet.tweet_id)
            tombstone_label = tweet_html.select_one(".Tombstone .Tombstone-label").text
            new_tweet.text = tombstone_label.strip()
            if "withheld in response to a report from the copyright holder" in new_tweet.text:
                # as per info in https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
                # “XY” - Content is withheld due to a DMCA request.
                takedown_type = "XY"
            else:
                takedown_type = "unknown"
            new_tweet.withheld_in = takedown_type
            new_tweet.timestamp = 0
            new_tweet.has_video = False
            new_tweet.image_count = 0
            new_tweet.favorites = 0
            new_tweet.retweets = 0
            new_tweet.replies = 0
            # favorites, retweets, replies and timestamp can be looked up through their api,
            # but original text and attachments are lost
            return new_tweet

        new_tweet.timestamp = int(tweet_html.select_one(".js-short-timestamp").get("data-time").strip())

        new_tweet.replying_to = None # need a second pass on specific threads to get reply chains
        qrt = tweet_html.select_one(".QuoteTweet-innerContainer")
        new_tweet.qrt_id = qrt.get("data-item-id").strip() if qrt else None

        poll_data, poll_finished = new_tweet._get_poll_data(tweet_html)
        new_tweet.poll_data = poll_data
        new_tweet.poll_finished = poll_finished

        new_tweet.has_video = bool(tweet_html.select(".js-stream-tweet .is-video"))
        new_tweet.image_count = len(tweet_html.select(".js-stream-tweet .AdaptiveMedia-photoContainer img"))
        replies = tweet_html.select_one(".ProfileTweet-action--reply .ProfileTweet-actionCount").get("data-tweet-stat-count")
        retweets = tweet_html.select_one(".ProfileTweet-action--retweet .ProfileTweet-actionCount").get("data-tweet-stat-count")
        favorites = tweet_html.select_one(".ProfileTweet-action--favorite .ProfileTweet-actionCount").get("data-tweet-stat-count")
        new_tweet.favorites = int(favorites)
        new_tweet.retweets = int(retweets)
        new_tweet.replies = int(replies)

        #new_tweet.links: List[str] = []
        new_tweet.embedded_link = new_tweet._get_embedded_link(tweet_html)
        new_tweet.text = new_tweet._get_tweet_text(tweet_html)

        #if not self.embedded_link and self.links and not self.image_count:
        #    LOGGER.debug("Using last link in post as an embed link in tweet %s", self.tweet_id)
        #    self.embedded_link = self.links[-1]
        return new_tweet


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
                    a = element.select_one("a")
                    element_text = a.text if a else ""
                elif "tweet-poi-geo-text" in element.attrs["class"]:
                    a = element.select_one("a")
                    poi_label = a.text
                    poi_id = a.get("data-place-id")
                    LOGGER.debug("encountered poi location data:%s, id:%s, tweet_id:%s",
                                 poi_label, poi_id, self.tweet_id)
                    self.poi = f"{poi_label}:{poi_id}"
                    element_text = ""
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
        if "twitter-atreply" in element.attrs["class"]:
            element_text = element.text
        elif "twitter-hashtag" in element.attrs["class"]:
            element_text = element.text
        elif "twitter-cashtag" in element.attrs["class"]:
            element_text = element.text
        elif "twitter-timeline-link" in element.attrs["class"]:
            if "data-expanded-url" in element.attrs:
                element_text = element.get("data-expanded-url")
                #self.links.append(element_text)
                if "u-hidden" in element.attrs["class"]:
                    # FIXME: decide what to do with withheld qrt links
                    # example: https://twitter.com/FakeUnicode/status/686654542574825473
                    if self.embedded_link:
                        #FIXME: decide whether embedded_link should always be the authoritative link
                        #tweetarchiver._untangle_link() line:420 card link = http://thehill.com/homenews/campaign/353673-biden-rich-are-as-patriotic-as-the-poor?amp#referrer=https://www.google.com&amp_tf=From%20%251$s
                        #tweetarchiver._untangle_link() line:421 hidden link = http://thehill.com/homenews/campaign/353673-biden-rich-are-as-patriotic-as-the-poor?amp#referrer=https://www.google.com&amp_tf=From%20%251%24s
                        # the two urls link to the same article despite the differring fragments
                        #FIXME: decide whether twitter's 'unsafe link warning' should be kept
                        # unsafe links direct to https://twitter.com/safety/unsafe_link_warning?unsafe_link={original_url}
                        # user is able to ignore the above warning and proceed to originally linked resource
                        # so far only seen this for ask.fm links and some vpns
                        if urlparse(self.embedded_link.rstrip("/")) != urlparse(element_text.rstrip("/")):
                            LOGGER.warning("HIDDEN URL AND TWITTR CARD URL DIFFER IN TWEET %s", self.tweet_id)
                            LOGGER.warning("card link = %s", self.embedded_link)
                            LOGGER.warning("hidden link = %s", element_text)

                    elif not self.qrt_id:
                        parsed_url = urlparse(element_text)
                        # do not warn for vine urls (RIP vine)
                        if parsed_url.netloc not in ("vine.co",):
                            LOGGER.warning("Using hidden timeline link as embed link, TWEET:%s , LINK:%s", self.tweet_id, element_text)
                        self.embedded_link = element_text
                    # else: this is a quote RT, embed link not needed
                    # V link is displayed as a twitter card only, do not add it to text
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
        card_container = tweet_html.select_one(".card2.js-media-container")
        if not card_container:
            return None

        #https://github.com/igorbrigadir/twitter-advanced-search

        card_name = card_container.get("data-card2-name")
        #card_name:poll2choice_text_only
        #card_name:poll3choice_text_only
        #card_name:poll4choice_text_only
        #card_name:poll2choice_image
        #card_name:poll3choice_image
        #card_name:poll4choice_image
        if card_name.startswith("poll"):
            # _get_poll_data already took care of this
            return None
        if card_name in ("promo_video_convo", "promo_image_convo"):
            #FIXME:handle amplify cards / promo_*_convo cards
            # HASHTAG START THE CONVERSATION
            # IN ALL MY YEARS OF USING TWITTER, NOT ONCE HAVE I SEEN THIS
            # https://business.twitter.com/en/help/campaign-setup/conversational-ad-formats.html
            # this usually puts a hidden timeline link in tweet, so the amplify card ends up as embedded link
            LOGGER.error("ADVERTISEMENT CARD FOUND IN TWEET %s, SKIPPING", self.tweet_id)
            return None
        if card_name == "2586390716:message_me":
            #FIXME: handle the private message shortcut
            # this usually puts a hidden timeline link in tweet, so it will end up as embedded link anyway
            LOGGER.error("FOUND PRIVATE MESSAGE SHORTCUT IN TWEET %s, SKIPPING", self.tweet_id)
            return None

        #card_name:promo_website

        #card_name:promo_image_app - this one does not display correctly/at all in web twitter
        #card_name:app

        #card_name:summary
        #card_name:summary_large_image

        #card_name:audio - cards of audio serving sites - for example soundcloud
        #card_name:player - youtube and others
        #card_name:animated_gif

        #LOGGER.error(card_container)
        frame_container = card_container.select_one("div")
        frame_url = frame_container.get("data-src")
        frame_url = f"https://twitter.com{frame_url}"

        LOGGER.debug("Downloading card frame from tweet %s", self.tweet_id)
        # authorization in form of referer header is required, otherwise 403 is returned
        frame_request = download(frame_url, headers={"Referer":f"https://twitter.com/user/status/{self.tweet_id}"})
        frame = BeautifulSoup(frame_request.response.text, HTML_PARSER)

        embedded_link = frame.select_one(".TwitterCard .TwitterCard-container").get("href")
        if not embedded_link:
            embedded_link = frame.select_one("a.js-openLink").get("href")
        if not embedded_link:
            LOGGER.error("Could not find embedded link for card '%s' in tweet %s", card_name, self.tweet_id)
            raise RuntimeError()

        # not all embedded links are shortened - this is rare but happens for some old tweets
        if embedded_link.startswith("https://t.co") or embedded_link.startswith("http://t.co"):
            if embedded_link.startswith("http:"):
                # avoid unnecessary redirects for links generated before t.co started fully encrypting traffic
                embedded_link = f"{'https'}{embedded_link[4:]}"
            #FIXME: handle http errors
            head_request = download(embedded_link, method="HEAD", allow_redirects=False)
            if head_request.response.is_redirect:
                LOGGER.debug("Detected redirect from '%s' to '%s'",
                             embedded_link, head_request.response.headers["location"])
                embedded_link = head_request.response.headers["location"]

        parsed_url = urlparse(embedded_link)
        if parsed_url.netloc == "twitter.com":
            # ignore twitter's warning, live on the edge
            if parsed_url.path == "/safety/unsafe_link_warning":
                embedded_link = parsed_url.query.split("=", maxsplit=1)[-1]
                LOGGER.debug("Ignoring unsafe link warning for url=%s", embedded_link)

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
        poll_frame = BeautifulSoup(poll_frame.response.text, HTML_PARSER)

        card_serialized = poll_frame.select_one("[type=\"text/twitter-cards-serialization\"]").text
        card_serialized = json.loads(card_serialized)["card"]
        poll_object["is_open"] = card_serialized["is_open"]
        if isinstance(poll_object["is_open"], str):
            poll_object["is_open"] = {"false":False, "true":True}[poll_object["is_open"].lower()]

        poll_object["choice_count"] = card_serialized["choice_count"]
        poll_object["end_time"] = timegm(time.strptime(card_serialized["end_time"], "%Y-%m-%dT%H:%M:%S%z"))
        # store time as unix timestamp for consistency ^

        poll_container = poll_frame.select_one(".TwitterCard .CardContent .PollXChoice")

        poll_object["winning_index"] = poll_container.get("data-poll-vote-majority")
        #poll_object["voted_for_index"] = poll_container.get("data-poll-user-choice")
        poll_choices = poll_container.select(".PollXChoice-choice .PollXChoice-choice--text")

        poll_object["votes_total"] = 0
        poll_object["choices"] = []
        for choice_num in range(poll_object["choice_count"]):
            choice_html = poll_choices[choice_num]
            choice = dict()
            choice["votes"] = int(card_serialized[f"count{choice_num+1}"])
            choice["votes_percent"] = choice_html.select_one(".PollXChoice-progress").text
            choice["label"] = choice_html.select_one("span:nth-of-type(2)").text
            poll_object["choices"].append(choice)
            poll_object["votes_total"] += choice["votes"]

        assert len(poll_object["choices"]) == poll_object["choice_count"]
        return poll_object, not poll_object["is_open"]


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
        LOGGER.debug("Scraping page %s : %s", page_number, query_url)
        # rate limit to 1 request per page_delay seconds
        time.sleep(max(0, loop_start + page_delay - time.time()))
        results_page = download(query_url).response.text
        results_page = BeautifulSoup(results_page, HTML_PARSER).select(".js-stream-tweet")
        loop_start = time.time()
        found_tweets = len(results_page)
        if found_tweets and found_tweets != 20:
            LOGGER.warning("Less than 20 results on this page (%s)", found_tweets)
            time.sleep(max(0, loop_start + page_delay - time.time()))
            results_page = download(query_url).response.text
            loop_start = time.time()
            results_page = BeautifulSoup(results_page, HTML_PARSER).select(".js-stream-tweet")
            if found_tweets != len(results_page):
                LOGGER.warning("Found %s tweets on the second try", len(results_page))
            else:
                LOGGER.warning("Same amount of tweets found on second attempt")

        max_id = 0
        new_tweets = []
        for tweet_html in results_page:
            # example of a tweet withheld due to copyright claim https://twitter.com/dodo/status/880524321390600192
            # FIXME: QRTs which are PART OF A THREAD and quote suspended accounts do not show up in search results
            # temporarily suspended accounts still show up in search results, but their contents
            # cannot be read - stop scraping immediately if such results show up
            # tweets withheld due to copyright notice show up as well but those are
            #FIXME: early exit can lead to gaps in archived tweets
            # should keep a record oftweet with highest id in database and last known good
            # tweet from current scraping session - if early exit is needed, store this info
            # in db and scrape that range again when/if account becomes readable again
            if "withheld-tweet" in tweet_html.attrs["class"]:
                tombstone_label = tweet_html.select_one(".js-stream-tweet .Tombstone .Tombstone-label")
                if tombstone_label and "account is temporarily unavailable" in tombstone_label.text:
                    LOGGER.error("This account has been suspended, content cannot be read, aborting!")
                    max_id = 0
                    new_tweets = []
                    break

            max_id = tweet_html.get("data-tweet-id").strip()
            new_tweets.append(tweet_html)
            tweets_found += 1

        yield new_tweets

        if not max_id:
            print("End reached, breaking")
            break

        page_number += 1
        if page_limit and page_number > page_limit:
            print(f"Page limit reached ({page_number})")
            break

        # do not include last seen tweet in next search
        max_id = int(max_id) - 1

    TWITTER_SESSION.close()

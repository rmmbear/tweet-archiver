import logging

import requests
from bs4 import BeautifulSoup as BS

from tweetarchiver import Tweet, download, HTML_PARSER

LOGGER = logging.getLogger(__name__)


COMPAREVARS = [
    "tweet_id",
    "thread_id",
    "timestamp",
    "account_id",
    "replying_to",
    "qrt_id",
    "poll_data",
    "poll_finished",
    "has_video",
    "image_count",
    "text",
    "poi",
    "withheld_in",
]

# if a value is not specified, it is assumed to be None
TEST_POLLS = [
    (
        #"https://twitter.com/FakeUnicode/status/1206075411794292736",
        "https://twitter.com/search?f=tweets&vertical=default&q=from:FakeUnicode since_id:1206075411794292735 max_id:1206075411794292736",
        Tweet(
            tweet_id=1206075411794292736, thread_id=1206075411794292736, timestamp=1576385760, account_id=2183231114,
            poll_data={"is_open": False, "choice_count": 4, "end_time": 1576472160, "winning_index": "3", "votes_total": 368,
                       "choices": [{"votes": 23, "votes_percent": "6%", "label": "SP"},
                                   {"votes": 110, "votes_percent": "30%", "label": "SQ"},
                                   {"votes": 167, "votes_percent": "45%", "label": "AB (ALBA)"},
                                   {"votes": 68, "votes_percent": "19%", "label": "GB (w/Wales, bye England)"}]},
            poll_finished=True, has_video=False, image_count=0,
            text="Scotland is (per ISO) a state of Great Britain, with 3166-2 code GB-SCT. If (when) it gains independence, what should its 3166-1 [https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2] code be?\n\nTaken: SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ\n\nReserved: SF SU\n\nFree: AB SP SQ SW\n\n#Poll")),
    (
        #"https://twitter.com/dril/status/1090496580413579265",
        "https://twitter.com/search?f=tweets&vertical=default&q=from:dril since_id:1090496580413579264 max_id:1090496580413579265",
        Tweet(
            tweet_id=1090496580413579265, thread_id=1090496580413579265, timestamp=1548829619, account_id=16298441,
            poll_data={"is_open": False, "choice_count": 2, "end_time": 1548829919, "winning_index": "1", "votes_total": 2660,
                       "choices": [{"votes": 1766, "votes_percent": "66%", "label": "the \"Follows you\" flair"},
                                   {"votes": 894, "votes_percent": "34%", "label": "the lauded \"Verfied\" mark"}]},
            poll_finished=True, has_video=False, image_count=0,
            text="What is it that you first seek when inspecting a profile which presents a potential networking opportunity")),
    (
        #"https://twitter.com/waypoint/status/876841985956597761",
        "https://twitter.com/search?f=tweets&vertical=default&q=from:waypoint since_id:876841985956597760 max_id:876841985956597761",
        Tweet(
            tweet_id=876841985956597761, thread_id=876841985956597761, timestamp=1497890395, account_id=2999703069,
            poll_data={"is_open": False, "choice_count": 3, "end_time": 1497976794, "winning_index": "3", "votes_total": 2604,
                       "choices": [{"votes": 988, "votes_percent": "38%", "label": "quote tweet"},
                                   {"votes": 526, "votes_percent": "20%", "label": "twote"},
                                   {"votes": 1090, "votes_percent": "42%", "label": "queet"}]},
            poll_finished=True, has_video=False, image_count=0,
            text="which one is the best?")),
]

LIVE_TEST_SETS = [
    TEST_POLLS
    #live_test_links
    #live_test_images
    #live_test_videos
    #live_test_gifs
    #live_test_qtweet
    #live_test_unicode_hell
]

def livetest():
    for test_set in LIVE_TEST_SETS:
        passed = False
        for url, expected_tweet in test_set:
            found_tweets = BS(download(url).response.text, HTML_PARSER)
            found_tweets = found_tweets.select(".js-stream-tweet")
            if not found_tweets:
                LOGGER.error("Test query did not return any tweets (%s)", url)
                continue
            if len(found_tweets) > 1:
                raise RuntimeError("Multiple tweets returned by test query, but only one was expected!")

            downloaded_tweet = Tweet.from_html(found_tweets[0])
            for var in COMPAREVARS:
                left = getattr(downloaded_tweet, var)
                right = getattr(expected_tweet, var)

                if left != right:
                    LOGGER.error("Live test error in tweet %s", expected_tweet.tweet_id)
                    LOGGER.error("varname    = %s", var)
                    LOGGER.error("Downloaded = %s", [left])
                    LOGGER.error("Expected   = %s", [right])

                    raise RuntimeError("Failed live parser test!")

                passed = True
                # we only need to test one tweet per category, but more is included
                # for redundancy - in case tweets/accounts get deleted/suspended
                break

        if not passed:
            raise RuntimeError("Could not retrieve any of the test tweets!")

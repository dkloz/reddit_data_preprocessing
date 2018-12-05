# __author__ = 'dimitrios'
"""Builds a dictionary from subreddit name to number of subscribers for that subreddit.
Data Downloaded 9/25.
Used http://redditmetrics.com/top/ to get this data
Also saves a file with the set of subreddits with more than X subscribers"""

import os
import random
import time
import requests

from util.io import save_pickle, load_pickle
from preprocessing.config_filenames import get_sub_dict_name, get_valid_sub_name


def get_most_popular(min_subscribers, subreddit_limit=50000, overwrite=False):
    """Reads, or crawls (if it does not exist) subreddit -> subscribers dictionary.
    If overwrite, it crawls the data from the internet again. Else, it reads the existing file.
    """
    subscribers_dict = {}
    subscribers_dict_filename = get_sub_dict_name(subreddit_limit)
    if os.path.exists(subscribers_dict_filename) and not overwrite:
        subscribers_dict = load_pickle(subscribers_dict_filename, False)
        return create_valid_subreddit_set(subscribers_dict, min_subscribers, overwrite)  # fast - run anyway

    subscribers_dict = crawl_subreddit_subscribers(subreddit_limit, subscribers_dict, subscribers_dict_filename)
    print '--> Subscriber Dict has %d entries' % len(subscribers_dict)

    return create_valid_subreddit_set(subscribers_dict, min_subscribers, overwrite)  # fast - run anyway


def crawl_subreddit_subscribers(subreddit_limit, subscribers_dict, subscribers_dict_filename):
    """Crawls dictionary from subreddit -> number of subscribers from http://redditmetrics.com/top/"""
    from lxml import html
    for page_offset in range(0, subreddit_limit, 100):
        url_str = 'http://redditmetrics.com/top/offset/%d' % page_offset
        print url_str, len(subscribers_dict)
        time.sleep(1 + 3 * random.random())
        page = requests.get(url_str)
        tree = html.fromstring(page.content)

        table = tree.xpath('//td[@class="tod"]')
        for i in range(100):
            # every second entry is name, every third is number of subscribers
            subscribers_dict[table[3 * i + 1].iterlinks().next()[2][3:]] = int(
                table[3 * i + 2].text.replace(',', ''))
    save_pickle(subscribers_dict_filename, subscribers_dict)
    return subscribers_dict


def create_valid_subreddit_set(subscribers_dict, subscriber_limit=1000, overwrite=False):
    """Make a set of subreddits with more than subscriber_limit subscribers, based on an input dictionary."""
    subreddit_set_filename = get_valid_sub_name(subscriber_limit)
    if os.path.exists(subreddit_set_filename) and not overwrite:
        return load_pickle(subreddit_set_filename, False)
    sub_set = set()  # get it?
    for (subreddit, subscriber_count) in subscribers_dict.iteritems():
        if subscriber_count >= subscriber_limit:
            sub_set.add(subreddit)
    print '-->Sub set has %d subreddits' % len(sub_set)
    save_pickle(subreddit_set_filename, sub_set)
    return sub_set


if __name__ == '__main__':
    crawl_subreddit_subscribers()

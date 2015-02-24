__author__ = 'sharvey'

from crawl.mysql.reddit import RedditCrawler


def create_crawler(type, **kwargs):
    if type == 'reddit':
        return RedditCrawler(**kwargs)


class Crawler(object):
    user_agent = ('corpus crawler by github.com/worldwise001')

    def __init__(self, user_agent=None):
        if user_agent is not None:
            self.user_agent = user_agent

    def crawl(self):
        pass
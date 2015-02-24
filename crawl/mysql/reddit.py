import mysql.connector
import praw
import pprint
import requests
import time
from datetime import datetime

import base36
from crawl import Crawler


class RedditCrawler(Crawler):
    r = None
    cnx = None

    def __init__(self, user_agent=None, mysql_options=None):
        super(RedditCrawler, self).__init__(user_agent)
        self.r = praw.Reddit(self.user_agent)
        if mysql_options is None:
            mysql_options = {}
        if 'user' not in mysql_options:
            mysql_options['user'] = 'reddit'
        if 'password' not in mysql_options:
            mysql_options['password'] = None
        if 'host' not in mysql_options or 'sock' not in mysql_options:
            mysql_options['host'] = 'localhost'
        if 'database' not in mysql_options:
            mysql_options['database'] = 'reddit'
        self.cnx = mysql.connector.connect(**mysql_options)

    def __get_submission(self, submission_id):
        submission = None
        while submission is None:
            try:
                submission = self.r.get_submission(submission_id=submission_id)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403 or e.response.status_code == 404:
                    return None
                else:
                    submission = None
                    continue
            except:
                submission = None
                continue
        return submission

    def __get_comments(self, submission):
        while True:
            try:
                submission.replace_more_comments(limit=None, threshold=0)
                return
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403 or e.response.status_code == 404:
                    return
                else:
                    continue
            except:
                continue

    def __get_author_name(self, entity):
        return entity.author.name if hasattr(entity, 'author') and entity.author is not None else None

    def __download_submission(self, sid):
        # Get submission
        submission = self.__get_submission(submission_id=sid)

        # Return nothing if there was a 404 (deleted) or 403 (forbidden) error
        if submission is None:
            return 0, 0

        # Get all the comments if possible
        self.__get_comments(submission)

        # Flatten the comment tree
        flat_comments = praw.helpers.flatten_tree(submission.comments)

        # Get the base subreddit ID
        subreddit_id = submission.subreddit_id[3:]

        # Get the author, if it is not [deleted]
        submission_author_name = self.__get_author_name(submission)

        cursor = self.cnx.cursor()

        # Insert the reddit name if it doesn't exist already
        cursor.execute('INSERT HIGH_PRIORITY IGNORE INTO `reddit` (`base36`, `name`) VALUES (%s, %s)',
                       (subreddit_id, submission.subreddit.display_name))

        # Insert the author if it doesn't exist already
        if submission_author_name is not None:
            cursor.execute('INSERT HIGH_PRIORITY IGNORE INTO `user` (`name`) VALUES (%s)', (submission_author_name, ))

        # Commit so we can link to this later
        self.cnx.commit()

        # Insert the submission data
        cursor.execute('''INSERT IGNORE INTO `submission` (
                        `base36`,
                        `title`,
                        `selftext`,
                        `selftext_html`,
                        `domain`,
                        `url`,
                        `ups`,
                        `downs`,
                        `created`,
                        `reddit_id`,
                        `user_id`)
                        VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        (SELECT `id` FROM `reddit` WHERE (`base36` = %s)),
                        (SELECT `id` FROM `user` WHERE (`name` = %s)))''',
                       (submission.id,
                        submission.title,
                        submission.selftext,
                        submission.selftext_html,
                        submission.domain,
                        submission.url,
                        submission.ups,
                        submission.downs,
                        datetime.utcfromtimestamp(submission.created_utc),
                        subreddit_id,
                        submission_author_name))

        # Commit so we can link to this later
        self.cnx.commit()

        # Attempt to insert all the possible authors found in the comments
        for comment in flat_comments:
            comment_author_name = self.__get_author_name(comment)
            if comment_author_name is not None:
                cursor.execute('INSERT HIGH_PRIORITY IGNORE INTO `user` (`name`) VALUES (%s)', (comment_author_name, ))

        # Commit so we can link to these later
        self.cnx.commit()

        # Insert the actual comments
        for comment in flat_comments:
            comment_author_name = self.__get_author_name(comment)
            cursor.execute('''INSERT IGNORE INTO `comment` (
                            `base36`,
                            `body`,
                            `body_html`,
                            `ups`,
                            `downs`,
                            `created`,
                            `user_id`,
                            `submission_id`)
                            VALUES (
                            %s, %s, %s, %s, %s, %s,
                            (SELECT `id` FROM `user` WHERE (`name` = %s)),
                            (SELECT `id` FROM `submission` WHERE (`base36` = %s)))''',
                           (comment.id,
                            comment.body,
                            comment.body_html,
                            comment.ups,
                            comment.downs,
                            datetime.utcfromtimestamp(comment.created_utc),
                            comment_author_name,
                            submission.id))

        # Commit ll the things
        self.cnx.commit()

        # Return a count
        return 1, len(flat_comments)

    def __wrap_download_submission(self, sid):
        try:
            return self.__download_submission(self, sid)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            return False

    def crawl(self, base36_start='2kxpiq', base36_end=None):
        sid_start = base36.decode(base36_start)
        if base36_end is not None:
            sid_end = base36.decode(base36_end)
        else:
            sid_end = None
        sid_it = sid_start

        post_count = 0
        comment_count = 0

        while sid_end is None or sid_it < sid_end:
            res = False
            while res is False:
                res = self.__wrap_download_submission(base36.encode(sid_it))
            post_count += res[0]
            comment_count += res[1]
            sid_it += 1





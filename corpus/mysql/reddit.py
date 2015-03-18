__author__ = 'sharvey'

import random

from corpus.mysql import MySQLCorpus
from corpus.mysql import util


class RedditMySQLCorpus(MySQLCorpus):

    def __init__(self, cpu_count=None):
        super(RedditMySQLCorpus, self).__init__(cpu_count)

    def __del__(self):
        super(RedditMySQLCorpus, self).__del__()

    def create(self):
        cursor = self.cnx.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `reddit` (
          `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          `base36` VARCHAR(16) NULL UNIQUE KEY,
          `name` VARCHAR(128) NOT NULL UNIQUE KEY
          );''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS `user` (
          `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          `base36` VARCHAR(16) NULL UNIQUE KEY,
          `name` VARCHAR(238) NOT NULL UNIQUE KEY,
          `created` DATETIME NULL
          );''')
        self.cnx.commit()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `submission` (
          `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          `base36` VARCHAR(16) NULL UNIQUE KEY,
          `title` VARCHAR(512),
          `selftext` TEXT,
          `selftext_html` TEXT,
          `domain` VARCHAR(64),
          `url` TEXT,
          `ups` INT NOT NULL,
          `downs` INT NOT NULL,
          `created` DATETIME NULL,
          `edited` DATETIME NULL,
          `reddit_id` BIGINT UNSIGNED NULL,
          `user_id` BIGINT UNSIGNED NULL,
          FOREIGN KEY (`reddit_id`) REFERENCES `reddit` (`id`) ON DELETE SET NULL ON UPDATE CASCADE,
          FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON DELETE SET NULL ON UPDATE CASCADE,
          INDEX (`domain`),
          INDEX (`created`),
          INDEX (`edited`)
          );''')
        self.cnx.commit()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `comment` (
          `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          `base36` VARCHAR(16) NULL UNIQUE KEY,
          `body` TEXT,
          `body_html` TEXT,
          `ups` INT NOT NULL,
          `downs` INT NOT NULL,
          `created` DATETIME NULL,
          `edited` DATETIME NULL,
          `user_id` BIGINT UNSIGNED NULL,
          `submission_id` BIGINT UNSIGNED NULL,
          FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON DELETE SET NULL ON UPDATE CASCADE,
          FOREIGN KEY (`submission_id`) REFERENCES `submission` (`id`) ON DELETE SET NULL ON UPDATE CASCADE,
          INDEX (`created`),
          INDEX (`edited`)
          );''')
        self.cnx.commit()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `submission_counts` (
          `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          `reddit_id` BIGINT UNSIGNED,
          `user_id` BIGINT UNSIGNED,
          `count` BIGINT UNSIGNED NOT NULL DEFAULT 0,
          `char_count` BIGINT UNSIGNED NOT NULL DEFAULT 0,
          FOREIGN KEY (`reddit_id`) REFERENCES `reddit` (`id`) ON DELETE SET NULL ON UPDATE CASCADE,
          FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON DELETE SET NULL ON UPDATE CASCADE,
          UNIQUE `pair` (`reddit_id`, `user_id`)
          );''')
        self.cnx.commit()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `comment_counts` (
          `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          `reddit_id` BIGINT UNSIGNED,
          `user_id` BIGINT UNSIGNED,
          `count` BIGINT UNSIGNED NOT NULL DEFAULT 0,
          `char_count` BIGINT UNSIGNED NOT NULL DEFAULT 0,
          FOREIGN KEY (`reddit_id`) REFERENCES `reddit` (`id`) ON DELETE SET NULL ON UPDATE CASCADE,
          FOREIGN KEY (`user_id`) REFERENCES `user` (`id`) ON DELETE SET NULL ON UPDATE CASCADE,
          UNIQUE `pair` (`reddit_id`, `user_id`)
          );''')
        self.cnx.commit()
        super(RedditMySQLCorpus, self).create_feature('comment')
        super(RedditMySQLCorpus, self).create_feature('submission')

    def load(self, source):
        pass

    def gen_counts(self):
        cursor = self.cnx.cursor(dictionary=True, buffered=True)
        cursor.execute('''SELECT DISTINCT `submission`.`reddit_id` AS `rid`, `comment`.`user_id` AS `uid` FROM `comment`
                        LEFT JOIN `submission` ON (`comment`.`submission_id`=`submission`.`id`)
                        WHERE `comment`.`user_id` IS NOT NULL AND `submission`.`reddit_id` IS NOT NULL''')
        results = cursor.fetchall()
        for row in results:
            rid = row['rid']
            uid = row['uid']
            cursor.execute('''INSERT IGNORE INTO `comment_counts`
                            (`reddit_id`, `user_id`, `count`, `char_count`)
                            SELECT
                            %d AS `reddit_id`,
                            %d AS `user_id`,
                            COUNT(`comment`.`id`) AS `count`,
                            SUM(LENGTH(`comment`.`body`)) AS `char_count`
                            FROM `comment`
                            LEFT JOIN `user` ON (`user`.`id`=`comment`.`user_id`)
                            LEFT JOIN `submission` ON (`submission`.`id`=`comment`.`submission_id`)
                            LEFT JOIN `reddit` ON (`reddit`.`id`=`submission`.`reddit_id`)
                            WHERE `submission`.`reddit_id` = %d AND `comment`.`user_id` = %d
                            ''' % (int(rid), int(uid), int(rid), int(uid)))
            self.cnx.commit()
        cursor.execute('''SELECT DISTINCT `submission`.`reddit_id` AS `rid`, `submission`.`user_id` AS `uid`
                        FROM `submission`
                        WHERE `submission`.`user_id` IS NOT NULL AND `submission`.`reddit_id` IS NOT NULL''')
        results = cursor.fetchall()
        for row in results:
            rid = row['rid']
            uid = row['uid']
            cursor.execute('''INSERT IGNORE INTO `submission_counts`
                            (`reddit_id`, `user_id`, `count`, `char_count`)
                            SELECT
                            %d AS `reddit_id`,
                            %d AS `user_id`,
                            COUNT(`submission`.`id`) AS `count`,
                            SUM(LENGTH(`submission`.`selftext`)) AS `char_count`
                            FROM `submission`
                            LEFT JOIN `user` ON (`user`.`id`=`submission`.`user_id`)
                            LEFT JOIN `reddit` ON (`reddit`.`id`=`submission`.`reddit_id`)
                            WHERE `submission`.`reddit_id` = %d AND `submission`.`user_id` = %d
                            ''' % (int(rid), int(uid), int(rid), int(uid)))
            self.cnx.commit()

    def gen_pos(self):
        # process submissions
        print('Generating parts of speech for submissions')
        src_columns = ['`id`', '`selftext`']
        src_table = '`submission`'
        transform_tuple_list = [ (util.extract_pos_s, ['`id`', '`pos`'], '`submission_pos`') ]
        super(RedditMySQLCorpus, self).process(src_columns, src_table, transform_tuple_list)

        # process comments
        print('Generating parts of speech for comments')
        src_columns = ['`id`', '`body`']
        src_table = '`comment`'
        transform_tuple_list = [ (util.extract_pos_c, ['`id`', '`pos`'], '`comment_pos`') ]
        super(RedditMySQLCorpus, self).process(src_columns, src_table, transform_tuple_list)

    def gen_stats(self):
        read_columns = [ '`id`',
                         '`ari`',
                         '`flesch_reading_ease`',
                         '`flesch_kincaid_grade_level`',
                         '`gunning_fog_index`',
                         '`smog_index`',
                         '`coleman_liau_index`',
                         '`lix`',
                         '`rix`' ]
        stat_columns = [ '`id`',
                         '`text_length_in_words`',
                         '`word_length_mean`',
                         '`word_length_median`',
                         '`word_length_variance`',
                         '`word_length_stdev`',
                         '`vocab_length`',
                         '`text_length_in_sentences`',
                         '`sentence_word_length_mean`',
                         '`sentence_word_length_median`',
                         '`sentence_word_length_variance`',
                         '`sentence_word_length_stdev`',
                         '`sentence_char_length_mean`',
                         '`sentence_char_length_median`',
                         '`sentence_char_length_variance`',
                         '`sentence_char_length_stdev`']

        # process submissions
        src_columns = ['`id`', '`selftext`']
        src_table = '`submission`'
        transform_tuple_list = [ (util.extract_stat_features_s, stat_columns, '`submission_feature_stat`'),
                                 (util.extract_read_features_s, read_columns, '`submission_feature_read`')]
        super(RedditMySQLCorpus, self).process(src_columns, src_table, transform_tuple_list)

        # process comments
        src_columns = ['`id`', '`body`']
        src_table = '`comment`'
        transform_tuple_list = [ (util.extract_stat_features_c, stat_columns, '`comment_feature_stat`'),
                                 (util.extract_read_features_c, read_columns, '`comment_feature_read`')]
        super(RedditMySQLCorpus, self).process(src_columns, src_table, transform_tuple_list)

    def gen_byte_ngram(self):
        src_columns = ['`id`', '`selftext`']
        src_table = '`submission`'
        transform_tuple_list = [ (util.discover_byte_ngrams_s, ['`gram`', '`size`'], '`byte_ngram`'),
                                 (util.discover_byte_cs_ngrams_s, ['`gram`', '`size`'], '`byte_cs_ngram`') ]
        super(RedditMySQLCorpus, self).process(src_columns, src_table, transform_tuple_list)

        src_columns = ['`id`', '`body`']
        src_table = '`comment`'
        transform_tuple_list = [ (util.discover_byte_ngrams_c, ['`gram`', '`size`'], '`byte_ngram`'),
                                 (util.discover_byte_cs_ngrams_c, ['`gram`', '`size`'], '`byte_cs_ngram`') ]
        super(RedditMySQLCorpus, self).process(src_columns, src_table, transform_tuple_list)

        pass

    def gen_word_ngram(self):
        pass

    def gen_pos_ngram(self):
        pass

    def get_user_list(self, corpus_type, char_count, reddits):
        cursor = self.cnx.cursor(dictionary=True, buffered=True)
        reddit_ids = []
        for r in reddits:
            cursor.execute('SELECT `id` FROM `reddit` WHERE `name`=%s', (r, ))
            results = cursor.fetchall()
            for result in results:
                reddit_ids.append(result['id'])

        query = '''SELECT DISTINCT `user`.`name` AS `username`, `user`.`id` AS `user_id` FROM `%s_counts` AS `a`
                        LEFT JOIN `user` ON (`user`.`id`=`a`.`user_id`)''' % corpus_type
        i = 0
        for rid in reddit_ids:
            subquery = ' LEFT JOIN `%s_counts` AS `r%d` ON (`a`.`user_id`=`r%d`.`user_id` AND `r%d`.`reddit_id`=%d)'\
                       % (corpus_type, i, i, i, rid)
            query += subquery
            i += 1

        for j in range(0, i):
            if j == 0:
                query += ' WHERE'
            else:
                query += ' AND'
            query += ' `r%d`.`char_count` > %d' % (j, char_count)

        print(query)
        cursor.execute(query)
        result = cursor.fetchall()
        return result

    def get_train_documents(self, corpus_type, user, reddit, char_count, min_char_count=0):
        cursor = self.cnx.cursor(dictionary=True, buffered=True)
        if (corpus_type == 'submission'):
            query = '''SELECT `submission`.`selftext` AS `text` FROM `submission`
                        LEFT JOIN `user` ON (`user`.`id`=`submission`.`user_id`)
                        LEFT JOIN `reddit` ON (`reddit`.`id`=`submission`.`reddit_id`)
                        WHERE `reddit`.`name`=%s AND `user`.`name`=%s AND LENGTH(`submission`.`selftext`) > %s'''
        elif (corpus_type == 'comment'):
            query = '''SELECT `comment`.`body` AS `text` FROM `comment`
                        LEFT JOIN `user` ON (`user`.`id`=`comment`.`user_id`)
                        LEFT JOIN `submission` ON (`submission`.`id`=`comment`.`submission_id`)
                        LEFT JOIN `reddit` ON (`reddit`.`id`=`submission`.`reddit_id`)
                        WHERE `reddit`.`name`=%s AND `user`.`name`=%s AND LENGTH(`comment`.`body`) > %s'''
        else:
            return None
        cursor.execute(query, (reddit, user, min_char_count))
        result = cursor.fetchall()
        text = ''
        for row in result:
            text += row['text'] + '\n'
        return text[:char_count]

    def get_test_documents(self, corpus_type, reddit, min_char_count=0):
        cursor = self.cnx.cursor(dictionary=True, buffered=True)
        if (corpus_type == 'submission'):
            query = '''SELECT `submission`.`id` AS `id`,
                            `user`.`name` AS `username`,
                            `submission`.`selftext` AS `text` FROM `submission`
                        LEFT JOIN `user` ON (`user`.`id`=`submission`.`user_id`)
                        LEFT JOIN `reddit` ON (`reddit`.`id`=`submission`.`reddit_id`)
                        WHERE `reddit`.`name`=%s AND LENGTH(`submission`.`selftext`) > %s'''
        elif (corpus_type == 'comment'):
            query = '''SELECT `comment`.`id` AS `id`,
                            `user`.`name` AS `username`,
                            `comment`.`body` AS `text` FROM `comment`
                        LEFT JOIN `user` ON (`user`.`id`=`comment`.`user_id`)
                        LEFT JOIN `submission` ON (`submission`.`id`=`comment`.`submission_id`)
                        LEFT JOIN `reddit` ON (`reddit`.`id`=`submission`.`reddit_id`)
                        WHERE `reddit`.`name`=%s AND LENGTH(`comment`.`body`) > %s'''
        else:
            return None
        cursor.execute(query, (reddit, min_char_count))
        result = cursor.fetchall()
        return result

    def get_test_grouped_documents(self, corpus_type, reddit, min_char_count=0):
        cursor = self.cnx.cursor(dictionary=True, buffered=True)
        if (corpus_type == 'submission'):
            query = '''SELECT `submission`.`id` AS `id`,
                            `user`.`name` AS `username`,
                            `submission`.`selftext` AS `text` FROM `submission`
                        LEFT JOIN `user` ON (`user`.`id`=`submission`.`user_id`)
                        LEFT JOIN `reddit` ON (`reddit`.`id`=`submission`.`reddit_id`)
                        WHERE `reddit`.`name`=%s AND LENGTH(`submission`.`selftext`) > %s'''
        elif (corpus_type == 'comment'):
            query = '''SELECT `comment`.`id` AS `id`,
                            `user`.`name` AS `username`,
                            `comment`.`body` AS `text` FROM `comment`
                        LEFT JOIN `user` ON (`user`.`id`=`comment`.`user_id`)
                        LEFT JOIN `submission` ON (`submission`.`id`=`comment`.`submission_id`)
                        LEFT JOIN `reddit` ON (`reddit`.`id`=`submission`.`reddit_id`)
                        WHERE `reddit`.`name`=%s AND LENGTH(`comment`.`body`) > %s'''
        else:
            return None
        cursor.execute(query, (reddit, min_char_count))
        result = cursor.fetchall()
        result0 = {}
        for r in result:
            if r['username'] is None:
                continue
            if r['username'] not in result0:
                result0[r['username']] = []
            result0[r['username']].append(r)
        return result0

    def get_test_subset_documents(self, corpus_type, reddit, user, min_char_count=0):
        cursor = self.cnx.cursor(dictionary=True, buffered=True)
        if (corpus_type == 'submission'):
            query = '''SELECT `submission`.`id` AS `id`,
                            `user`.`name` AS `username`,
                            `submission`.`selftext` AS `text` FROM `submission`
                        LEFT JOIN `user` ON (`user`.`id`=`submission`.`user_id`)
                        LEFT JOIN `reddit` ON (`reddit`.`id`=`submission`.`reddit_id`)
                        WHERE `reddit`.`name`=%s AND LENGTH(`submission`.`selftext`) > %s AND `user`.`name` = %s'''
        elif (corpus_type == 'comment'):
            query = '''SELECT `comment`.`id` AS `id`,
                            `user`.`name` AS `username`,
                            `comment`.`body` AS `text` FROM `comment`
                        LEFT JOIN `user` ON (`user`.`id`=`comment`.`user_id`)
                        LEFT JOIN `submission` ON (`submission`.`id`=`comment`.`submission_id`)
                        LEFT JOIN `reddit` ON (`reddit`.`id`=`submission`.`reddit_id`)
                        WHERE `reddit`.`name`=%s AND LENGTH(`comment`.`body`) > %s AND `user`.`name` = %s'''
        else:
            return None
        cursor.execute(query, (reddit, min_char_count, user))
        result = cursor.fetchall()
        return result
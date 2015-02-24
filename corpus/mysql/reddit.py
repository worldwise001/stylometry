__author__ = 'sharvey'

from corpus.mysql import MySQLCorpus
from corpus.mysql import util


class RedditMySQLCorpus(MySQLCorpus):

    def __init__(self, **kwargs):
        super(RedditMySQLCorpus, self).__init__(**kwargs)

    def __del__(self):
        super(RedditMySQLCorpus, self).__del__(self)

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
        (RedditMySQLCorpus, self).create_feature('comment')

    def load(self, source):
        pass

    def gen_counts(self):
        pass

    def gen_pos(self):
        # process submissions
        src_columns = ['`id`', '`selftext`']
        src_table = '`submission`'
        transform_tuple_list = [ (util.extract_pos_s, ['`selftext_pos`'], '`submission_pos`') ]
        (MySQLCorpus, self).process(src_columns, src_table, transform_tuple_list)

        # process comments
        src_columns = ['`id`', '`body`']
        src_table = '`comment`'
        transform_tuple_list = [ (util.extract_pos_s, ['`body_pos`'], '`comment_pos`') ]
        (MySQLCorpus, self).process(src_columns, src_table, transform_tuple_list)

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
        (MySQLCorpus, self).process(src_columns, src_table, transform_tuple_list)

        # process comments
        src_columns = ['`id`', '`body`']
        src_table = '`column`'
        transform_tuple_list = [ (util.extract_stat_features_c, stat_columns, '`comment_feature_stat`'),
                                 (util.extract_read_features_c, read_columns, '`comment_feature_read`')]
        (MySQLCorpus, self).process(src_columns, src_table, transform_tuple_list)

    def gen_byte_ngram(self):
        pass

    def gen_word_ngram(self):
        pass

    def gen_pos_ngram(self):
        pass

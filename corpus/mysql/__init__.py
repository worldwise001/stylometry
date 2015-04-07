__author__ = 'sharvey'

import multiprocessing
import mysql.connector
import sys
import time
import traceback

from corpus import Corpus


class MySQLCorpus(Corpus):
    cnx = None

    def __init__(self, cpu_count=None):
        super(MySQLCorpus, self).__init__(cpu_count)

    def __del__(self):
        if self.cnx is not None:
            self.cnx.commit()
            self.cnx.close()
        super(MySQLCorpus, self).__del__()

    def setup(self, **mysql_options):
        self.cnx = mysql.connector.connect(**mysql_options)

    def create_feature(self, table_name):
        cursor = self.cnx.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `%s_feature_read` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY,
          `ari` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `flesch_reading_ease` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `flesch_kincaid_grade_level` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `gunning_fog_index` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `smog_index` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `coleman_liau_index` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `lix` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `rix` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          FOREIGN KEY (`id`) REFERENCES `%s` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
        );''' % (table_name, table_name))

        cursor.execute('''CREATE TABLE IF NOT EXISTS `%s_feature_stat` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY,
          `text_length_in_words` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `word_length_mean` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `word_length_median` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `word_length_variance` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `word_length_stdev` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `vocab_length` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `text_length_in_sentences` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `sentence_word_length_mean` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `sentence_word_length_median` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `sentence_word_length_variance` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `sentence_word_length_stdev` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `sentence_char_length_mean` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `sentence_char_length_median` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `sentence_char_length_variance` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          `sentence_char_length_stdev` DECIMAL(65,30) NOT NULL DEFAULT 0.0,
          FOREIGN KEY (`id`) REFERENCES `%s` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
        );''' % (table_name, table_name))
        self.cnx.commit()

        cursor.execute('''CREATE TABLE IF NOT EXISTS `word` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
          `word` VARCHAR(190) NOT NULL UNIQUE
        );''')
        self.cnx.commit()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `word_ngram` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
          `size` BIGINT UNSIGNED NOT NULL,
          `n1_id` BIGINT UNSIGNED NULL,
          `n2_id` BIGINT UNSIGNED NULL,
          `n3_id` BIGINT UNSIGNED NULL,
          `n4_id` BIGINT UNSIGNED NULL,
          `n5_id` BIGINT UNSIGNED NULL,
          `n6_id` BIGINT UNSIGNED NULL,
          `n7_id` BIGINT UNSIGNED NULL,
          UNIQUE `ngram` (`size`, `n1_id`, `n2_id`, `n3_id`, `n4_id`, `n5_id`, `n6_id`, `n7_id`),
          FOREIGN KEY (`n1_id`) REFERENCES `word` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n2_id`) REFERENCES `word` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n3_id`) REFERENCES `word` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n4_id`) REFERENCES `word` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n5_id`) REFERENCES `word` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n6_id`) REFERENCES `word` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n7_id`) REFERENCES `word` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          INDEX(`size`)
        );''')
        self.cnx.commit()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `%s_word_ngram` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
          `%s_id` BIGINT UNSIGNED NOT NULL,
          `ngram_id` BIGINT UNSIGNED NOT NULL,
          `count` BIGINT UNSIGNED NOT NULL,
          UNIQUE `ngram` (`%s_id`, `ngram_id`),
          FOREIGN KEY (`%s_id`) REFERENCES `%s` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`ngram_id`) REFERENCES `word_ngram` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
        );''' % (table_name, table_name, table_name, table_name, table_name))
        self.cnx.commit()

        cursor.execute('''CREATE TABLE IF NOT EXISTS `word_clean` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
          `word` VARCHAR(190) NOT NULL UNIQUE
        );''')
        self.cnx.commit()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `word_clean_ngram` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
          `size` BIGINT UNSIGNED NOT NULL,
          `n1_id` BIGINT UNSIGNED NULL,
          `n2_id` BIGINT UNSIGNED NULL,
          `n3_id` BIGINT UNSIGNED NULL,
          `n4_id` BIGINT UNSIGNED NULL,
          `n5_id` BIGINT UNSIGNED NULL,
          `n6_id` BIGINT UNSIGNED NULL,
          `n7_id` BIGINT UNSIGNED NULL,
          UNIQUE `ngram` (`size`, `n1_id`, `n2_id`, `n3_id`, `n4_id`, `n5_id`, `n6_id`, `n7_id`),
          FOREIGN KEY (`n1_id`) REFERENCES `word_clean` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n2_id`) REFERENCES `word_clean` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n3_id`) REFERENCES `word_clean` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n4_id`) REFERENCES `word_clean` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n5_id`) REFERENCES `word_clean` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n6_id`) REFERENCES `word_clean` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`n7_id`) REFERENCES `word_clean` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          INDEX(`size`)
        );''')
        self.cnx.commit()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `%s_word_clean_ngram` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
          `%s_id` BIGINT UNSIGNED NOT NULL,
          `ngram_id` BIGINT UNSIGNED NOT NULL,
          `count` BIGINT UNSIGNED NOT NULL,
          UNIQUE `ngram` (`%s_id`, `ngram_id`),
          FOREIGN KEY (`%s_id`) REFERENCES `%s` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`ngram_id`) REFERENCES `word_clean_ngram` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
        );''' % (table_name, table_name, table_name, table_name, table_name))
        self.cnx.commit()

        cursor.execute('''CREATE TABLE IF NOT EXISTS `byte_ngram` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
          `size` BIGINT UNSIGNED NOT NULL,
          `gram` VARCHAR(190) NOT NULL UNIQUE,
          INDEX(`size`)
        );''')
        self.cnx.commit()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `%s_byte_ngram` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
          `%s_id` BIGINT UNSIGNED NOT NULL,
          `ngram_id` BIGINT UNSIGNED NOT NULL,
          `count` BIGINT UNSIGNED NOT NULL,
          UNIQUE `ngram` (`%s_id`, `ngram_id`),
          FOREIGN KEY (`%s_id`) REFERENCES `%s` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`ngram_id`) REFERENCES `byte_ngram` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
        );''' % (table_name, table_name, table_name, table_name, table_name))
        self.cnx.commit()

        cursor.execute('''CREATE TABLE IF NOT EXISTS `byte_cs_ngram` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
          `size` BIGINT UNSIGNED NOT NULL,
          `gram` VARCHAR(190) NOT NULL UNIQUE,
          INDEX(`size`)
        );''')
        self.cnx.commit()
        cursor.execute('''CREATE TABLE IF NOT EXISTS `%s_byte_cs_ngram` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
          `%s_id` BIGINT UNSIGNED NOT NULL,
          `ngram_id` BIGINT UNSIGNED NOT NULL,
          `count` BIGINT UNSIGNED NOT NULL,
          UNIQUE `ngram` (`%s_id`, `ngram_id`),
          FOREIGN KEY (`%s_id`) REFERENCES `%s` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
          FOREIGN KEY (`ngram_id`) REFERENCES `byte_cs_ngram` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
        );''' % (table_name, table_name, table_name, table_name, table_name))
        self.cnx.commit()

        cursor.execute('''CREATE TABLE IF NOT EXISTS `%s_pos` (
          `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY,
          `pos` TEXT,
          FOREIGN KEY (`id`) REFERENCES `%s` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
          );''' % (table_name, table_name))
        self.cnx.commit()

    def run_sql(self, query, params):
        cursor = self.cnx.cursor(dictionary=True)
        while True:
            try:
                #print(query, params)
                cursor.execute(query, params)
                if cursor.with_rows:
                    #print('Getting rows')
                    return cursor.fetchall()
                return None
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback)

    def make_query(self, columns, table, condition=None, condition_params=None, limit=None, joins=None):
        query = 'SELECT %s FROM %s'
        if table is None:
            return None
        if columns is None:
            columns_txt = '*'
        else:
            columns_txt = ', '.join(columns)
        query = query % (columns_txt, table)
        if joins is not None:
            query += ' ' + joins

        if condition is not None:
            query += ' WHERE ' + condition

        if limit is not None:
            query = query + ' LIMIT %d, %d' % (limit[0], limit[1])
        return query

    def select_rows(self, columns, table, condition=None, condition_params=None, limit=None, joins=None):
        query = 'SELECT %s FROM %s'
        if table is None:
            return None
        if columns is None:
            columns_txt = '*'
        else:
            columns_txt = ', '.join(columns)
        query = query % (columns_txt, table)
        if joins is not None:
            query += ' ' + joins

        if condition is not None:
            query += ' WHERE ' + condition

        if limit is not None:
            print(query + ' LIMIT %d, %d' % (limit[0], limit[1]))
        else:
            print(query)

        cursor = self.cnx.cursor(dictionary=True, buffered=True)
        if limit is None:
            cursor.execute(query, condition_params)
            returned_total = None
            if cursor.with_rows:
                returned_total = cursor.fetchall()
        else:
            returned_total = []
            start = limit[0]
            end = limit[1]
            chunk = 100
            j = start
            while j < end:
                subquery = '%s LIMIT %d, %d' % (query, j, chunk)
                try:
                    cursor.execute(subquery, condition_params)
                    returned = cursor.fetchall()
                    if len(returned) == 0:
                        j += chunk
                    else:
                        j += len(returned)
                        returned_total.extend(returned)
                except (KeyboardInterrupt, SystemExit):
                    raise
                except:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_exception(exc_type, exc_value, exc_traceback)
                    time.sleep(2)
                    self.cnx.reconnect(10, 5)
                    chunk = int(chunk*.8)
                    continue
        return returned_total

    def insert_rows(self, columns, table, tuple_list):
        query = 'INSERT IGNORE INTO %s (%s) VALUES (%s)'
        if table is None:
            return None
        if columns is None:
            columns_txt = '*'
        else:
            columns_txt = ','.join(columns)
        values = ['%s'] * len(columns)
        values_txt = ','.join(values)
        query = query % (table, columns_txt, values_txt)

        print(query)

        cursor = self.cnx.cursor(dictionary=True, buffered=True)
        chunk = 10
        i = 1
        for atuple in tuple_list:
            while True:
                try:
                    if type(atuple) is list:
                        for aatuple in atuple:
                            cursor.execute(query, aatuple)
                    else:
                        cursor.execute(query, atuple)
                    i += 1
                    if i % chunk == 0:
                        self.cnx.commit()
                    break
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    traceback.print_exception(exc_type, exc_value, exc_traceback)
                    time.sleep(2)
                    self.cnx.reconnect(10, 5)
                    continue
        self.cnx.commit()

    def process(self, src_columns, src_table, transform_tuple_list):
        cpus = multiprocessing.cpu_count()
        chunk = 1000
        j = 0
        fp = open('data/oldresults.csv', 'w')
        st = time.clock()
        while True:
            query = self.make_query(src_columns, src_table, limit=(j, chunk))
            cursor = self.cnx.cursor(dictionary=True, buffered=True)
            cursor.execute(query, None)
            if cursor.with_rows:
                rows = cursor.fetchall()
            else:
                rows = []
            if len(rows) == 0:
                break
            for ttuple in transform_tuple_list:
                transform, dst_columns, dst_table = ttuple
                it = self.pool.imap_unordered(transform, rows, 100)
                tuple_list = []
                while True:
                    try:
                        atuple = it.next()
                        tuple_list.append(atuple)
                    except StopIteration:
                        break
                self.insert_rows(dst_columns, dst_table, tuple_list)
            j += chunk
            et = time.clock()
            fp.write('%s, %s\n' % (j, (et-st)))
            fp.flush()
        fp.close()

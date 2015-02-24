__author__ = 'sharvey'

import multiprocessing


class Corpus(object):
    pool = None

    def __init__(self, cpu_count=None):
        if cpu_count is None:
            cpu_count = multiprocessing.cpu_count()
        self.pool = multiprocessing.Pool(cpu_count)

    def __del__(self):
        self.pool.close()
        self.pool.join()

    def create(self):
        pass

    def load(self, source):
        pass

    def gen_counts(self):
        pass

    def gen_pos(self):
        pass

    def gen_stats(self):
        pass

    def gen_byte_ngram(self):
        pass

    def gen_word_ngram(self):
        pass

    def gen_pos_ngram(self):
        pass
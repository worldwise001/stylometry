__author__ = 'sharvey'

import multiprocessing

from corpus.mysql.reddit import RedditMySQLCorpus
from feature import ngram
from feature import lexical
import cred

import pprint

def gen_feature(atuple):
    aset = set()
    bngram = ngram.get_byte_ngrams(atuple['text'])
    for n in bngram['ngram_byte']:
        for k in bngram['ngram_byte'][n]:
            aset.add(('nb%d' % n, k))
    for n in bngram['ngram_byte_cs']:
        for k in bngram['ngram_byte_cs'][n]:
            aset.add(('nbcs%d' % n, k))
    wngram = ngram.get_word_ngrams(atuple['text'])
    for n in wngram['ngram_word']:
        for k in wngram['ngram_word'][n]:
            aset.add(('nw%d' % n, ' '.join(k)))
    for n in wngram['ngram_word_clean']:
        for k in wngram['ngram_word_clean'][n]:
            aset.add(('nwc%d' % n, ' '.join(k)))
    words, clean_words = ngram.get_words(atuple['text'])
    for word in words:
        aset.add(('w', word))
    for word in clean_words:
        aset.add(('cw', word))
    lex = lexical.get_symbol_dist(atuple['text'])
    for k in lex['lex']:
        aset.add(('l', k))
    #pprint.pprint(aset)
    return set(aset)

if __name__ == '__main__':
    corpus = RedditMySQLCorpus()
    corpus.setup(**(cred.kwargs))
    corpus.create()
    pool = multiprocessing.Pool(1) #multiprocessing.cpu_count())
    print('set up pool')

    chunk = 100
    j = 0
    feature_set = set()
    while True:
        print(j)
        rows = corpus.run_sql('SELECT `body` AS `text` FROM `comment` LIMIT %d, %d' % (j, chunk), None)
        if len(rows) == 0:
            break
        it = pool.imap_unordered(gen_feature, rows, 100)
        new_feature_set = set()
        while True:
            try:
                atuple = it.next()
                new_feature_set = new_feature_set.union(atuple)
            except StopIteration:
                break
        new_feature_set.difference_update(feature_set)
        pprint.pprint(len(new_feature_set))
        corpus.run_sqls('INSERT IGNORE INTO `feature_map` (`type`, `feature`) VALUES (%s, %s)', list(new_feature_set))
        corpus.cnx.commit()
        feature_set = feature_set.union(new_feature_set)
        j += chunk
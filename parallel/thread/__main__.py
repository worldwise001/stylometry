__author__ = 'sharvey'

from parallel.thread import DataGetThread
from parallel.thread import DataPutThread
from parallel.thread import TaskThread
from parallel.thread import tprint

import feature.simple

import time
import multiprocessing
from corpus.mysql.reddit import RedditMySQLCorpus

from main import cred


def fn(arg):
    st = time.clock()
    read = feature.simple.get_read_stats(arg['text'].encode('ascii', 'ignore'))
    et = time.clock()
    #tprint(('stats took %s' % (et - st)))
    return (arg['id'],
            read['ari'],
            read['flesch_reading_ease'],
            read['flesch_kincaid_grade_level'],
            read['gunning_fog_index'],
            read['smog_index'],
            read['coleman_liau_index'],
            read['lix'],
            read['rix'])

corpus1 = RedditMySQLCorpus()
corpus1.setup(**(cred.kwargs))
corpus1.create()

corpus2 = RedditMySQLCorpus()
corpus2.setup(**(cred.kwargs))
corpus2.create()

st = time.clock()

dgt = DataGetThread(corpus1, 'SELECT id, body AS text FROM comment', None, limit=1000)
tt = []
dgt.start()
for i in range(0, multiprocessing.cpu_count()):
    t = TaskThread(fn)
    tt.append(t)
    t.start()

dpt = DataPutThread(corpus2, 'INSERT IGNORE INTO tc_read'
                             '(id, ari, flesch_reading_ease, flesch_kincaid_grade_level,'
                             'gunning_fog_index, smog_index, coleman_liau_index, lix, rix)'
                             'VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s)')
dpt.start()

dgt.join()
tprint(('dgt join'))

for t in tt:
    t.join()
    tprint(('task join'))

dpt.join()
tprint(('dpt join'))

et = time.clock()

print('time elapsed: %s' % (et-st))
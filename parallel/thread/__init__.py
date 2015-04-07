__author__ = 'sharvey'

from threading import Thread
import threading
import traceback
import sys
import time

queue = []
queue_lock = threading.Lock()
queue_event = threading.Event()

result = []
result_lock = threading.Lock()
result_event = threading.Event()

exit_event = threading.Event()

end_event = threading.Event()

data_chunk = 1000
process_chunk = 100

print_lock = threading.Lock()


def tprint(tuple):
    with print_lock:
        print(tuple)


class DataGetThread(Thread):
    corpus = None
    query = None
    params = None
    limit = 0

    def __init__(self, corpus, query, params, limit=0):
        self.corpus = corpus
        self.query = query
        self.params = params
        super(DataGetThread, self).__init__()

    def run(self):
        tprint((self.ident, 'Data Get Thread Start'))
        start = 0
        j = start
        chunk_size = data_chunk
        while not exit_event.is_set() and (self.limit == 0 or j < self.limit):
            subquery = '%s LIMIT %d, %d' % (self.query, j, chunk_size)
            try:
                chunk = self.corpus.run_sql(subquery, self.params)
                if len(chunk) == 0:
                    break
                with queue_lock:
                    queue.extend(chunk)
                    queue_event.set()
                j += len(chunk)
                #tprint(('Data Get: %s' % j))
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback)
                time.sleep(2)
                self.cnx.reconnect(10, 5)
                chunk_size = int(chunk_size*.8)
                continue
        queue_event.set()
        end_event.set()
        tprint((self.ident, 'Data Get Thread Done'))


class DataPutThread(Thread):
    corpus = None
    query = None

    def __init__(self, corpus, query):
        self.corpus = corpus
        self.query = query
        super(DataPutThread, self).__init__()

    def run(self):
        count = 0
        tprint((self.ident, 'Data Put Thread Start'))
        while True:
            try:
                #tprint((self.ident, 'result_event.wait()'))
                result_event.wait()
                #tprint((self.ident, 'wait for result lock'))
                with result_lock:
                    #tprint((self.ident, 'acquire result lock'))
                    if exit_event.is_set() and len(result) == 0:
                        break
                    chunk = result[:data_chunk]
                    del result[:data_chunk]
                    if len(result) == 0 and not exit_event.is_set():
                        result_event.clear()
                #tprint((self.ident, 'release result lock'))
                for ch in chunk:
                    self.corpus.run_sql(self.query, ch)
                count += len(chunk)
                #tprint(('Data Put: %s' % count))
                self.corpus.cnx.commit()
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback)
                time.sleep(2)
                self.cnx.reconnect(10, 5)
                continue
        tprint((self.ident, 'Data Put Thread Done'))

class TaskThread(Thread):
    fn = None

    def __init__(self, fn):
        self.fn = fn
        super(TaskThread, self).__init__()

    def run(self):
        tprint((self.ident, 'Task Thread Start'))
        while not exit_event.is_set():
            #tprint((self.ident, 'queue_event.wait()'))
            queue_event.wait()
            #tprint((self.ident, 'wait for queue lock'))
            with queue_lock:
                #tprint((self.ident, 'acquire queue lock'))
                chunk = queue[:process_chunk]
                del queue[:process_chunk]
                if len(queue) == 0:
                    if end_event.is_set():
                        exit_event.set()
                    else:
                        queue_event.clear()
            #tprint((self.ident, 'release queue lock'))
            local_result = []
            for i in chunk:
                #tprint((self.ident, 'append'))
                local_result.append(self.fn(i))
            #tprint((self.ident, 'wait for result lock'))
            with result_lock:
                #tprint((self.ident, 'acquire result lock'))
                if len(result) == 0:
                    result_event.set()
                result.extend(local_result)
            tprint((self.ident, 'processed %s' % len(local_result)))
            #tprint((self.ident, 'release result lock'))
        tprint((self.ident, 'Task Thread Done'))
        result_event.set()

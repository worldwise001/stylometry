import multiprocessing
import pprint
from ppm import Trie

trie = None

def test(a):
    x, trie = a
    trie.add('b')
    trie.add('b')
    print(x, trie.bit_encoding)
    return trie.bit_encoding

if __name__ == '__main__':
    trie = Trie(5)
    trie.add('a')
    alist = [ (x, trie) for x in range(0, multiprocessing.cpu_count()) ]
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    result = pool.map(test, alist)
    pprint.pprint(result)

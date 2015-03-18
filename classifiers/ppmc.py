__author__ = 'sharvey'

from classifiers import Classifier
from corpus.mysql.reddit import RedditMySQLCorpus
from ppm import Trie


class RedditPPM(Classifier):
    trie = None

    def train(self, document, order=5):
        if (self.trie is not None):
            del self.trie
        self.trie = Trie(order)
        for c in document:
            self.trie.add(c)

    def test(self, documents):
        results = []
        for row in documents:
            results.append({'id': row['id'],
                            'username': row['username'],
                            'label': (self.user == row['username']),
                            'score': self.score(row['text'])})
        return results

    def score(self, text):
        test_bits = 0
        newtrie = self.trie.duplicate()
        document = text.encode('utf-8')
        for c in document:
            newtrie.add(c)
            test_bits += newtrie.bit_encoding
        del newtrie
        return test_bits/(len(document)*8)

    def __str__(self):
        return '%f %f' % (self.trie.bit_encoding, self.trie.probability_encoding)
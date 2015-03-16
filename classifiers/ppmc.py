__author__ = 'sharvey'

from classifiers import Classifier
from corpus.mysql.reddit import RedditMySQLCorpus
from ppm import Trie


class RedditPPM(Classifier):
    corpus = None
    trie = None
    user = None
    reddit = None
    order = 5

    def __init__(self, corpus):
        self.corpus = corpus

    def train(self, corpus_type, user, reddit, char_count, order=5):
        if (self.trie is not None):
            del self.trie
        self.trie = Trie(order)
        self.reddit = reddit
        self.user = user
        document = self.corpus.get_train_documents(corpus_type, user, reddit, char_count).encode('utf-8')
        for c in document:
            self.trie.add(c)

    def test(self, corpus_type, reddit, char_count):
        documents = self.corpus.get_test_documents(corpus_type, reddit)
        results = []
        for row in documents:
            test_bits = 0
            newtrie = self.trie.duplicate()
            document = row['text'].encode('utf-8')
            for c in document:
                newtrie.add(c)
                test_bits += newtrie.bit_encoding
            del newtrie
            results.append({'id': row['id'],
                            'username': row['username'],
                            'label': (self.user == row['username']),
                            'score': test_bits/(len(document)*8)})
        return results

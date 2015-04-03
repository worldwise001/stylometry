import graph
from corpus.mysql.reddit import RedditMySQLCorpus

import cred

if __name__ == '__main__':
    corpus = RedditMySQLCorpus()
    corpus.setup(**(cred.kwargs))
    corpus.create()

    result = corpus.run_sql('SELECT ari FROM comment_feature_read')
    values = [ result[i]['ari'] for i in result ]
    graph.hist('data/ari_hist', values, 'ARI', 'Frequency', 'Frequency of ARI values')
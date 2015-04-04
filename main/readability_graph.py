import graph
from corpus.mysql.reddit import RedditMySQLCorpus

import cred

if __name__ == '__main__':
    corpus = RedditMySQLCorpus()
    corpus.setup(**(cred.kwargs))

    indices = ['ari', 'flesch_reading_ease', 'flesch_kincaid_grade_level', 'gunning_fog_index', 'smog_index',
               'coleman_liau_index', 'lix', 'rix']

    for i in indices:
        result = corpus.run_sql('SELECT COUNT(*) AS count, FLOOR(FLOOR(%s)/10.0)*10 AS bin '
                                     'FROM comment_feature_read '
                                     'GROUP BY bin ORDER BY bin' % i, None)

        values = [ (r[i], r['count']) for r in result ]
        graph.hist_prebin('data/%s_hist' % i, values, i, 'Frequency',
                          'Frequency of %s values' % i)

    result = corpus.run_sql('SELECT * FROM comment_feature_read', None)

    seen = []
    limits = {
        'ari': (-20, 100),
        'flesch_reading_ease': (-150, 200),
        'flesch_kincaid_grade_level': (-20, 50),
        'gunning_fog_index': (0, 40),
        'smog_index': (0, 20),
        'coleman_liau_index': (-30, 30),
        'lix': (0, 100),
        'rix': (0, 10)
    }
    for i in indices:
        for j in indices:
            if i == j:
                continue
            key = tuple(sorted([i, j]))
            if key in seen:
                continue
            seen.append(key)
            x = [ float(v[i]) for v in result ]
            y = [ float(v[j]) for v in result ]
            graph.scatter('data/%s-%s' % (i, j), x, y, limits[i], limits[j], i, j)
import graph
from corpus.mysql.reddit import RedditMySQLCorpus

import cred

if __name__ == '__main__':
    corpus = RedditMySQLCorpus()
    corpus.setup(**(cred.kwargs))

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

    indices = ['ari', 'flesch_reading_ease', 'flesch_kincaid_grade_level', 'gunning_fog_index', 'smog_index',
               'coleman_liau_index', 'lix', 'rix']

    for i in indices:
        bin_width = (limits[i][1] - limits[i][0])/20
        result = corpus.run_sql('SELECT COUNT(*) AS count, FLOOR(FLOOR(%s)/%s)*%s AS bin '
                                     'FROM comment_feature_read '
                                     'GROUP BY bin ORDER BY bin' % (i, bin_width, bin_width), None)
        print(i)

        old_values = [ (r['bin'], r['count']) for r in result ]
        values = []
        for a, b in old_values:
            if a >= limits[i][0] and a <= limits[i][1]:
                values.append((a, b))
        graph.hist_prebin('data/%s_hist' % i, values, bin_width, i, 'Frequency',
                          'Frequency of %s values' % i)

    result = corpus.run_sql('SELECT * FROM comment_feature_read ORDER BY RAND() LIMIT 2000', None)

    seen = []
    for i in indices:
        for j in indices:
            if i == j:
                continue
            key = tuple(sorted([i, j]))
            if key in seen:
                continue
            print(key)
            seen.append(key)
            x = [ float(v[i]) for v in result ]
            y = [ float(v[j]) for v in result ]
            graph.scatter('data/%s-%s' % (i, j), x, y, True, limits[i], limits[j], i, j)

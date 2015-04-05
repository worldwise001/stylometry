from feature import simple
import graph

from nltk.corpus import brown


def calc_readability(corpus):
    texts = []
    results = []
    for fileid in corpus.fileids():
        sentlist = brown.sents(fileids=[fileid])
        text = ' '.join([ ' '.join(ss) for ss in sentlist ])
        texts.append(text)
    for text in texts:
        results.append(simple.get_text_stats(text)['read'])
    return results

if __name__ == '__main__':
    result = calc_readability(brown)

    values = [ float(v['ari']) for v in result ]
    graph.hist('data/ari_hist', values, 'ARI', 'Frequency',
               'Frequency of ARI values')

    values = [ float(v['flesch_reading_ease']) for v in result ]
    graph.hist('data/flesch_reading_ease_hist', values, 'Flesch Reading Ease', 'Frequency',
               'Frequency of Flesch Reading Ease values')

    values = [ float(v['flesch_kincaid_grade_level']) for v in result ]
    graph.hist('data/flesch_kincaid_grade_level_hist', values, 'Flesch Kincaid Grade Level', 'Frequency',
               'Frequency of Flesch Kincaid Grade Level values')

    values = [ float(v['gunning_fog_index']) for v in result ]
    graph.hist('data/gunning_fog_index_hist', values, 'Gunning Fog Index', 'Frequency',
               'Frequency of Gunning Fog Index values')

    values = [ float(v['smog_index']) for v in result ]
    graph.hist('data/smog_index_hist', values, 'Smog Index', 'Frequency',
               'Frequency of Smog Index values')

    values = [ float(v['coleman_liau_index']) for v in result ]
    graph.hist('data/coleman_liau_index_hist', values, 'Coleman Liau Index', 'Frequency',
               'Frequency of Coleman Liau Index values')

    values = [ float(v['lix']) for v in result ]
    graph.hist('data/lix_hist', values, 'LIX', 'Frequency',
               'Frequency of LIX values')

    values = [ float(v['rix']) for v in result ]
    graph.hist('data/rix_hist', values, 'RIX', 'Frequency',
               'Frequency of RIX values')

    indices = ['ari', 'flesch_reading_ease', 'flesch_kincaid_grade_level', 'gunning_fog_index', 'smog_index',
               'coleman_liau_index', 'lix', 'rix']
    seen = []
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
            graph.scatter('data/%s-%s' % (i, j), x, y, True, x_title=i, y_title=j)
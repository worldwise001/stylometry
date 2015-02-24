from nltk import word_tokenize
import nltk.data

from readability import Readability

import pprint
import string
import numpy
import math

numpy.seterr(divide='ignore', invalid='ignore')
sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')


def clean_words(words):
    new_words = []
    for word in words:
        test_word1 = word.rstrip(string.punctuation)
        test_word2 = word.lstrip(string.punctuation)
        test_word3 = word.strip(string.punctuation)
        if test_word3 == '' or test_word2 == '' or test_word1 == '':
            continue
        new_words.append(word.lstrip(string.punctuation).rstrip(string.punctuation).lower())
    return new_words


def get_text_stats(text):
    stats = { 'read': {}, 'stat': {}}

    # readability stats
    rd = Readability(text)
    stats['read']['ari'] = rd.ARI()
    stats['read']['flesch_reading_ease'] = rd.FleschReadingEase()
    stats['read']['flesch_kincaid_grade_level'] = rd.FleschKincaidGradeLevel()
    stats['read']['gunning_fog_index'] = rd.GunningFogIndex()
    stats['read']['smog_index'] = rd.SMOGIndex()
    stats['read']['coleman_liau_index'] = rd.ColemanLiauIndex()
    stats['read']['lix'] = rd.LIX()
    stats['read']['rix'] = rd.RIX()

    # perform some statistics on words
    words = word_tokenize(text)
    words_lens = [ len(word) for word in words ]
    clean = clean_words(words)
    clean_lens = [ len(word) for word in clean ]

    stats['stat']['text_length_in_words'] = len(clean)
    stats['stat']['word_length_mean'] = float(numpy.mean(clean_lens))
    stats['stat']['word_length_median'] = float(numpy.median(clean_lens))
    stats['stat']['word_length_variance'] = float(numpy.var(clean_lens))
    stats['stat']['word_length_stdev'] = float(numpy.std(clean_lens))

    # vocab length
    vocab = set(clean)
    stats['stat']['vocab_length'] = len(vocab)

    # sentence stats
    sentences_raw = sent_detector.tokenize(text.strip())
    sentences = [ clean_words(word_tokenize(sentence)) for sentence in sentences_raw ]
    sentences_word_lens = [ len(sentence) for sentence in sentences ]
    sentences_char_lens = [ len(sentence) for sentence in sentences_raw ]

    stats['stat']['text_length_in_sentences'] = len(sentences)
    stats['stat']['sentence_word_length_mean'] = float(numpy.mean(sentences_word_lens))
    stats['stat']['sentence_word_length_median'] = float(numpy.median(sentences_word_lens))
    stats['stat']['sentence_word_length_variance'] = float(numpy.var(sentences_word_lens))
    stats['stat']['sentence_word_length_stdev'] = float(numpy.std(sentences_word_lens))
    stats['stat']['sentence_char_length_mean'] = float(numpy.mean(sentences_char_lens))
    stats['stat']['sentence_char_length_median'] = float(numpy.median(sentences_char_lens))
    stats['stat']['sentence_char_length_variance'] = float(numpy.var(sentences_char_lens))
    stats['stat']['sentence_char_length_stdev'] = float(numpy.std(sentences_char_lens))

    newstats = {}
    for key in stats['stat']:
        if math.isnan(stats['stat'][key]):
            newstats[key] = 0
        else:
            newstats[key] = stats['stat'][key]

    stats['stat'] = newstats

    return stats

if __name__ == '__main__':
    text_stats = get_text_stats('''(How does it deal with this parenthesis?)
    "It should be part of the previous sentence."
    "(And the same with this one.)" ('And this one!') "('(And (this)) '?)" [(and this. )] Good muffins cost $3.88
    in New York.  Please buy me\ntwo of them.\n\nThanks.''')

    pprint.pprint(text_stats)

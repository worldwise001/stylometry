from collections import defaultdict
import pprint

from nltk import word_tokenize

import simple


def get_byte_ngram(text, n=2, cs=False):
    if not cs:
        text = text.lower()
    ngrams = defaultdict(int)
    for i in range(0, len(text)-n+1):
        ng = text[i:i+n]
        ngrams[ng] += 1
    return ngrams


def get_word_ngram(text, n=2, clean=False):
    ngrams = defaultdict(int)
    words = word_tokenize(text)
    if clean:
        words = simple.clean_words(words)
    for i in range(0, len(words)-n+1):
        ng = tuple(words[i:i+n])
        ngrams[ng] += 1
    return ngrams


def get_word_ngrams(text):
    ngram_dict = {}
    for n in range(2, 8):
        ngrams = get_word_ngram(text, n)
        ngram_dict['ngram_word%d' % n] = ngrams
        ngrams = get_word_ngram(text, n, True)
        ngram_dict['ngram_word%d_clean' % n] = ngrams
    return ngram_dict


def get_byte_ngrams(text):
    ngram_dict = {}
    for n in range(2, 8):
        ngrams = get_byte_ngram(text, n)
        ngram_dict['ngram_byte%d' % n] = ngrams
        ngrams = get_byte_ngram(text, n, True)
        ngram_dict['ngram_byte%d_cs' % n] = ngrams
    return ngram_dict

if __name__ == '__main__':
    text = '''Newspapers in India are classified into two categories according to the amount and completeness of
    information in them. Newspapers in the first category have more information and truth. Those in the second category
    do not have much information and sometimes they hide the truth. Newspapers in the first category have news
    collected from different parts of the country and also from different countries. They also have a lot of sports and
    business news and classified ads. The information they give is clear and complete and it is supported by showing
    pictures. The best know example of this category is the Indian Express. Important news goes on the first page with
    big headlines, photographs from different angles, and complete information. For example, in 1989-90, the Indian
    prime minister, Rajive Ghandi, was killed by a terrorist using a bomb. This newspaper investigated the situation
    and gave information that helped the CBI to get more support. They also showed diagrams of the area where the prime
    minister was killed and the positions of the bodies after the attack. This helped the reader understand what
    happened. Unlike newspaper in the first category, newspapers in the second category do not give as much
    information. They do not have international news, sports, or business news and they do not have classified ads.
    Also, the news they give is not complete. For example, the newspaper Hindi gave news on the death of the prime
    minister, but the news was not complete. The newspaper didn't investigate the terrorist group or try to find out
    why this happened. Also, it did not show any pictures from the attack or give any news the next day. It just gave
    the news when it happened, but it didn't follow up. Therefore, newspapers in the first group are more popular than
    those in the second group.'''
    pprint.pprint(get_byte_ngrams(text))
    pprint.pprint(get_word_ngrams(text))

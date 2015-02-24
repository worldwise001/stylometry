import feature


def extract_stat_features_s(row):
    return extract_stat_features(row, 'selftext')


def extract_stat_features_c(row):
    return extract_stat_features(row, 'body')


def extract_stat_features(row, text_column):
    text = row[text_column].encode('ascii', 'ignore')
    if text == '':
        return (row['id'], 0.0, 0.0, 0.0, 0.0, 0.0,
                 0.0, 0.0, 0.0, 0.0, 0.0,
                 0.0, 0.0, 0.0, 0.0, 0.0)
    else:
        stats = feature.simple.get_text_stats(text)
        return (row['id'],
                stats['stat']['text_length_in_words'],
                stats['stat']['word_length_mean'],
                stats['stat']['word_length_median'],
                stats['stat']['word_length_variance'],
                stats['stat']['word_length_stdev'],
                stats['stat']['vocab_length'],
                stats['stat']['text_length_in_sentences'],
                stats['stat']['sentence_word_length_mean'],
                stats['stat']['sentence_word_length_median'],
                stats['stat']['sentence_word_length_variance'],
                stats['stat']['sentence_word_length_stdev'],
                stats['stat']['sentence_char_length_mean'],
                stats['stat']['sentence_char_length_median'],
                stats['stat']['sentence_char_length_variance'],
                stats['stat']['sentence_char_length_stdev'])


def extract_read_features_s(row):
    return extract_read_features(row, 'selftext')


def extract_read_features_c(row):
    return extract_read_features(row, 'body')


def extract_read_features(row, text_column):
    text = row[text_column].encode('ascii', 'ignore')
    if text == '':
        return (row['id'], 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    else:
        stats = feature.simple.get_text_stats(text)
        return (row['id'],
                stats['read']['ari'],
                stats['read']['flesch_reading_ease'],
                stats['read']['flesch_kincaid_grade_level'],
                stats['read']['gunning_fog_index'],
                stats['read']['smog_index'],
                stats['read']['coleman_liau_index'],
                stats['read']['lix'],
                stats['read']['rix'])

def extract_pos_s(row):
    return extract_pos(row, 'selftext')

def extract_pos_c(row):
    return extract_pos(row, 'body')

def extract_pos(row, text_column):
    text = row[text_column]
    pos = feature.pos.text_to_pos(text)
    return (row['id'], pos)
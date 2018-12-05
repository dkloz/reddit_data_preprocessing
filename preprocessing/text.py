"""A file to parse text from json files and similar functions."""

import os
import time
import io
import numpy as np
import multiprocessing as mp
import json

from preprocessing.config_filenames import n_proc
from util.preprocessing_util import set_to_dict, combine_dicts, is_valid_entry
from util.text_util import entry_to_tokens, tokenize_sent_words, replace_with_ids, simplify_post
from util.io import save_pickle, load_pickle, save_txt, save_text_sentences, make_go_rw


def json2vocab(filenames, vocab_filename, vocab_size, valid_users=None, valid_subreddits=None, overwrite=False):
    """Reads all the .json files and keeps the top words mentioned in them by the valid users and subreddits.

    Args:
        filenames: list of paths where the .json files are.
        vocab_filename: String with the path of the vocabulary.
        vocab_size: Total number of words to be used, ie top-k limit.
        valid_users: Set of users whose words should be kept.
        valid_subreddits: Set of subreddits whose words should be kept.
        overwrite: Whether to overwrite existing file.
    Returns:
        A set of words.
    Saves:
        A set of words and a text file with word, count (for sanity check).
    """

    counter_filename = vocab_filename.replace('pkl', 'txt')

    if os.path.exists(vocab_filename) and not overwrite:
        return load_pickle(vocab_filename, False)

    print 'Making:\n%s\n%s' % (vocab_filename, counter_filename)
    counters = []
    limit = vocab_size

    pool = mp.Pool(n_proc)
    proc_data_size = int(np.ceil(1. * len(filenames) / n_proc))
    for i in range(n_proc):
        proc_filenames = filenames[i * proc_data_size:(i + 1) * proc_data_size]
        if len(proc_filenames) > 0:
            pool.apply_async(_json2vocab_mp,
                             args=(i, proc_filenames, valid_subreddits, valid_users), callback=counters.append)
    pool.close()
    pool.join()

    combined_counters = combine_dicts(counters)
    print 'Total words before pruning were %d' % len(combined_counters)
    sorted_vocab = sorted(combined_counters.items(), key=lambda x: x[1], reverse=True)

    vocab = set([x[0] for x in sorted_vocab[:limit - 3]])
    # explicity add unk and sentence start and end tokens.
    vocab.add('<unk>')
    vocab.add('<sent_end>')
    vocab.add('<sent_start>')
    vocab = set_to_dict(vocab, 1)  # word ids start from 1!!
    save_pickle(vocab_filename, vocab)

    final_counter = np.array(sorted_vocab[:limit])
    save_txt(counter_filename, final_counter, delimiter='  ', fmt='%s')
    return vocab


def _json2vocab_mp(proc_id, filenames, subreddits, users):
    vocab = {}
    tokens = ''
    for filename in filenames:
        print '--->%d Doing %s' % (proc_id, filename)

        with open(filename, 'r') as f:
            i = 0
            limit = 1
            start_time = time.time()
            for line in f:
                i += 1
                if i % limit == 0:
                    time_passed = time.time() - start_time
                    print '\t%d %d posts, unique word tokens: %d, time passed: %02f' % (
                        proc_id, i, len(vocab), time_passed)
                    limit *= 2
                entry = json.loads(line)
                try:
                    if is_valid_entry():  # this needs to be filled out by user!
                        tokens = entry_to_tokens(entry)

                        for t in tokens:
                            vocab[t] = vocab.get(t, 0) + 1
                except Exception as e:
                    print '%d: Exception in %s' % (proc_id, entry['body'])
                    print tokens
                    print e.message
                    return
    return vocab


def json2text(filenames, text_filename, valid_users=None, valid_subreddits=None, years=None, overwrite=False):
    """Reads all the .json files creates a file with the text of users, subreddits.

    The resulting file has the format "user_name, subreddit_name, text". The text has no \n's so it can safely
    be saved as a text file with newline separators.
    Args:
        filenames: list of paths where the .json files are.
        text_filename: filename where text should be saved.
        valid_users: Set of users whose words should be kept.
        valid_subreddits: Set of subreddits whose words should be kept.
        years: list containing which years this should run on.
        overwrite: Whether to overwrite existing file.
    Returns:
        A set of words
    Saves:
        A set of words and a text file with word, count (for sanity check)

    """
    if os.path.exists(text_filename) and not overwrite:
        print 'File: %s already exists' % text_filename
        return

    print 'Getting all the text for %d users and %d subreddits' % (len(valid_users), len(valid_subreddits))
    sentences = []

    pool = mp.Pool(n_proc)
    proc_data_size = int(np.ceil(1. * len(filenames) / n_proc))
    for i in range(n_proc):
        proc_filenames = filenames[i * proc_data_size:(i + 1) * proc_data_size]
        if len(proc_filenames) > 0:
            pool.apply_async(_json2text_mp,
                             args=(i, proc_filenames, valid_subreddits, valid_users), callback=sentences.extend)
    pool.close()
    pool.join()

    print 'Total sentences (posts): %d' % len(sentences)
    save_text_sentences(text_filename, sentences)


def _json2text_mp(proc_id, filenames, subreddits, users):
    """Replaces multiple appearances of \n with one. Then replaces all \n with a ."""
    sentences = []
    for filename in filenames:
        print '--->%d Doing %s' % (proc_id, filename)

        with open(filename, 'r') as f:
            i = 0
            limit = 1
            start_time = time.time()
            for line in f:
                i += 1
                if i % limit == 0:
                    time_passed = time.time() - start_time
                    print '\t%d %d posts, valid sentences: %d, time passed: %.2f' % (
                        proc_id, i, len(sentences), time_passed)
                    limit *= 2

                entry = json.loads(line)
                if is_valid_entry():  # write this!
                    text = simplify_post(entry['body']).encode('utf-8')
                    user = entry['author']
                    subreddit = entry['subreddit']
                    sentences.append('%s\t%s\t%s' % (user, subreddit, text))
    return sentences


def text2ids(text_filename, text_id_filename, vocab, valid_users=None, valid_subreddits=None, overwrite=False):
    """Wrapper for conversion of a text file into a file with ids (user_ids, subreddit_ids, word_ids).

    Args:
        text_filename: filename where text exists.
        text_id_filename: filename where ids should be saved.
        vocab: Dictionary from word -> word_id.
        valid_users: Set of valid usernames.
        valid_subreddits: Set of valid subreddits.
        overwrite: Whether to overwrite existing file.
    """

    if not os.path.exists(text_filename):
        print 'The file %s has to be created first!. Returning' % text_filename
        return

    if os.path.exists(text_id_filename) and not overwrite:
        print 'File: %s already exists' % text_id_filename
        return

    print 'Making: %s' % text_id_filename
    users = set_to_dict(valid_users, start=1)
    subreddits = set_to_dict(valid_subreddits, start=1)

    _text2ids_conversion(text_filename, text_id_filename, users, subreddits, vocab)


def _text2ids_conversion(source_filename, target_filename, users, subreddits, vocab):
    """Converts a text file with format user\t subreddit\t text to the same format with ids. Also splits into sentences.

    The new format is 'user_id\t subreddit_id\t sentence1\t sentence2\t.... \n
    sentence: is in format word_id1 word_id2 etc.
    Args:
        source_filename: Path to file with text.
        target_filename: Path of file that result is going to be saved at.
        users: Dictionary from username to user id.
        subreddits: Dictionary from subreddit name to subreddit it.
        vocab: Dictionary from word to word id.
    """
    total_sentences = 0
    valid_posts = 0
    lim = 1
    start_time = time.time()
    with io.open(source_filename, 'r', encoding='utf-8') as fr:
        with open(target_filename, 'w') as fw:
            for line in fr:
                if valid_posts % lim == 0:
                    time_passed = time.time() - start_time
                    print 'Valid posts so far: %d in %.02f sec' % (valid_posts, time_passed)
                    lim *= 2
                user_name, subreddit_name, text = line.split('\t')
                user = users[user_name]
                subreddit = subreddits[subreddit_name]
                sentences = tokenize_sent_words(text)
                sentences = [replace_with_ids(s, vocab) for s in sentences]
                sentences = [s for s in sentences if len(s) > 0]  # remove empty ones.
                if len(sentences):  # remove empty posts
                    fw.write('%d\t%d' % (user, subreddit))
                    valid_posts += 1
                    for s in sentences:
                        total_sentences += 1
                        fw.write('\t%s' % ' '.join(map(str, s)))
                    fw.write('\n')
            fw.close()
        fr.close()
    make_go_rw(target_filename)

    time_passed = time.time() - start_time
    print 'Valid posts: %d --> Total sentences: %d in %.02f sec' % (valid_posts, total_sentences, time_passed)


def json2ids(filenames, params, vocab, valid_users=None, valid_subreddits=None, years=None, overwrite=False):
    """End to end conversion of json files, to file with user_id, subreddit_id, text.

    Args:
        filenames: list of paths where the .json files are.
        params: parameters that define how many words will be kept.
        vocab: Dictionary from word -> word_id.
        valid_users: Set of users whose words should be kept.
        valid_subreddits: Set of subreddits whose words should be kept.
        years: list containing which years this should run on.
        overwrite: Whether to overwrite existing file.

    """
    json2text(filenames, params, valid_users, valid_subreddits, years, overwrite)
    text2ids(params, vocab, valid_users, valid_subreddits, years, overwrite)

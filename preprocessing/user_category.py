# __author__ = 'dimitrios'
"""Reads a lot of files downloaded from http://files.pushshift.io/reddit/comments/
An example of a .json entry is
{"name":"t1_c02chew","body":"Some of the linux distros, as well as BSD, make this really easy. You don't need to tweak anything, it's \"ready to compile\". I don't bother with it myself.","subreddit_id":"t5_6","distinguished":null,"ups":2,"id":"c02chew","downs":0,"author_flair_css_class":null,"subreddit":"reddit.com","score":2,"gilded":0,"created_utc":"1193875218","link_id":"t3_5zjl1","controversiality":0,"edited":false,"parent_id":"t1_c02ch4f","score_hidden":false,"author":"BraveSirRobin","author_flair_text":null,"retrieved_on":1427424835,"archived":true}
"""
import os
import sys
import time

import multiprocessing as mp
import numpy as np
import simplejson as json

from preprocessing.subreddit_popularity import get_most_popular
from preprocessing.config_filenames import n_proc, get_all_uc_dict_filenames
from util.preprocessing_util import set_to_dict, is_valid_entry, data_to_sparse, sparse_to_data_array
from util.io import save_pickle, load_pickle, save_array, load_array


def json2dicts(filenames, params, overwrite=False):
    """Uses multiple processes to convert .json files to dictionaries.

    Creates one dictionary from username-> post count
    and one from username+subreddit -> count.
    It only keeps SOME subreddits, those with at least x subscribers. x is defined in params.
    Args:
        filenames: list of paths with .json files
        params: preprocessing parameters. Used to identify which subreddits to keep (based on min number of subscribers.
        overwrite: Boolean that dictates whether to overwrite existing file (if it exists),
    """
    print '--> Converting %d files with %d processes' % (len(filenames), n_proc)
    subreddits_to_keep = get_most_popular(params.min_subscribers)

    pool = mp.Pool(n_proc)
    proc_data_size = int(np.ceil(1. * len(filenames) / n_proc))
    for i in range(n_proc):
        proc_filenames = filenames[i * proc_data_size:(i + 1) * proc_data_size]
        if len(proc_filenames) > 0:
            pool.apply_async(_json2dicts_multiple_files,
                             args=(i, proc_filenames, params, subreddits_to_keep, overwrite))
    pool.close()
    pool.join()

    sys.stdout.flush()


def _json2dicts_multiple_files(proc_id, filenames, params, subreddits_to_keep, overwrite=False):
    for filename in filenames:
        _json2dicts_mp(proc_id, filename, params, subreddits_to_keep, params, overwrite)


def _json2dicts_mp(proc_id, filename, user_count_filename, uc_dict_filename, params, overwrite=False):
    """Processes a file as downloaded from the reddit data webpage: http://files.pushshift.io/reddit/comments/

    For each file it creates two files. One with a dictionary from user -> count and one from user_category -> count.
    """
    print '\t%d Converting %s for at least %d subscribers' % (
        proc_id, os.path.basename(filename), params.min_subscribers),

    if os.path.exists(uc_dict_filename) and not overwrite:
        print ': exists! Moving on'
        return
    print
    count_dict = {}
    user_post_count = {}

    f = open(filename, 'r')
    i = 0
    limit = 1
    start_time = time.time()
    for line in f:
        i += 1
        if i % limit == 0:
            time_passed = time.time() - start_time
            print '\t%d %d posts, unique user-subreddit pairs: %d, time passed: %02f' % (
                proc_id, i, len(count_dict), time_passed)
            limit *= 2

        entry = json.loads(line)
        if is_valid_entry():  # write this
            k = '%s %s' % (entry['author'], entry['subreddit'])  # key is author + ' ' + subreddit
            count_dict[k] = count_dict.get(k, 0) + 1
            user_post_count[entry['author']] = user_post_count.get(entry['author'], 0) + 1

    time_passed = time.time() - start_time
    print '\t%d %d posts, size of uc_dict: %d, time passed: %02f' % (proc_id, i, len(count_dict), time_passed)
    print '\t%d %d users' % (proc_id, len(user_post_count))
    save_pickle(uc_dict_filename, count_dict)
    save_pickle(user_count_filename, user_post_count)


####


def json2matrix(filenames, result_filename, valid_subreddits, valid_users, first_level_only=False):
    """Reads json files from filenames and only keeps entries from valid subreddits and valid users.

    Args:
        filenames: list of paths with .json files to be read
        result_filename: string with path of file to be saved.
        valid_subreddits: set of subreddit names to keep
        valid_users: set of users to keep
        first_level_only: boolean to decide whether to only keep first level comments (ie not indented).
    Returns:
        a CSR matrix of counts, user x subreddits."""

    print 'User cat matrix from %d users and %d subreddits' % (len(valid_users), len(valid_subreddits))

    if os.path.exists(result_filename):
        return data_to_sparse(load_array(result_filename, False))
    else:
        print '--> Making %s from %d .json files with %d processes' % (result_filename, len(filenames), n_proc)
        sys.stdout.flush()

        proc_data_size = int(np.ceil(1. * len(filenames) / n_proc))
        proc_data = []
        for i in range(n_proc):
            proc_data.append(filenames[i * proc_data_size:(i + 1) * proc_data_size])

        results = []
        pool = mp.Pool(n_proc)
        for i in range(n_proc):
            if len(proc_data[i]) > 0:
                pool.apply_async(_json2matrix_mp,
                                 args=(i, proc_data[i], valid_subreddits, valid_users, first_level_only),
                                 callback=results.append)
        pool.close()
        pool.join()

        data_array = np.zeros((0, 3))
        for r in results:
            data_array = np.vstack((data_array, r))

        data_array = sparse_to_data_array(data_to_sparse(data_array))  # consolidate
        save_array(result_filename, data_array)
    return data_to_sparse(data_array)


def _json2matrix_mp(proc_id, filenames, valid_subreddits, valid_users):
    """MP part that reads json files from filenames and only keeps entries from valid subreddits and valid users.
    Args:
        proc_id: process id
        filenames: list of paths with .json files to be read
        valid_subreddits: set of subreddit names to keep
        valid_users: set of users to keep
    Returns:
        a COO matrix of counts, user x subreddits.
    """
    users = set_to_dict(valid_users)
    subreddits = set_to_dict(valid_subreddits)
    data = np.zeros((0, 3))

    for filename in filenames:
        print proc_id, filename
        f = open(filename, 'r')
        i = 1
        start_time = time.time()
        for line in f:
            entry = json.loads(line)
            if is_valid_entry():  # write this
                u = users[entry['author']]
                c = subreddits[entry['subreddit']]
                data = np.vstack((data, [u, c, 1]))
                if len(data) % 10000 == 0 and len(data) > 0:
                    data = sparse_to_data_array(data_to_sparse(data))  # consolidate

            i += 1
            if i % 1000000 == 0:
                time_passed = time.time() - start_time
                print '\t%d %d posts, data:%d->%d, time: %.3f' % (
                    proc_id, i, len(data), np.sum(data[:, 2]), time_passed)
    return data


####

def dict2matrix(params, coo_data_filename, valid_subreddits=None, valid_users=None, years=None, overwrite=False):
    """Converts dictionaries of user-category counts to a single UxC matrix.

    Args:
        params: parameters of preprocessing that define where to find dictionaries.
        coo_data_filename: path where the COO data will be saved.
        valid_subreddits: set of subreddits to be considered. If none, it will be loaded from where params dictates.
        valid_users: set of users to be considered. If none, it will be loaded from where params dictates.
        years: list of all the years we want to take into consideration. If none, it selects all available.
        overwrite: Boolean that dictates whether to overwrite existing file (if it exists).
        """

    print '--> Making %s' % coo_data_filename,

    if os.path.exists(coo_data_filename) and not overwrite:
        print 'exists'
        return load_array(coo_data_filename, False)
    else:

        to_remove = None

        user_cat_counts_filenames = get_all_uc_dict_filenames(params, years)
        sys.stdout.flush()

        proc_data_size = int(np.ceil(1. * len(user_cat_counts_filenames) / n_proc))
        proc_data = []
        for i in range(n_proc):
            proc_data.append(user_cat_counts_filenames[i * proc_data_size:(i + 1) * proc_data_size])

        results = []
        pool = mp.Pool(n_proc)
        for i in range(n_proc):
            if len(proc_data[i]) > 0:
                pool.apply_async(_dict2matrix_mp, args=(i, proc_data[i], valid_subreddits, valid_users, to_remove),
                                 callback=results.append)
        pool.close()
        pool.join()

        data = np.zeros((0, 3))

        for r in results:
            data = np.vstack((data, r))
        R, C = len(valid_users), len(valid_subreddits)

        data = np.vstack((data, np.array([R - 1, C - 1, 0])))

        print 'Total entries in UxS matrix: %d -> %d' % (len(data), len(np.where(data[:, 2] > 0)[0]))
        save_array(coo_data_filename,
                   sparse_to_data_array(data_to_sparse(data)))  # do this to avoid multiple counts
        return sparse_to_data_array(data_to_sparse(data))


def _dict2matrix_mp(proc_id, counts_filenames, valid_subreddits, valid_users, to_remove=None):
    """ Convert a list of dictionaries into a COO array.

    Map splits user-cat in user and cat and returns tuple: (user, cat, count)
    Filter makes sure user is in valid users and cat in valid categories.
    (user and cat are strings and are turned into id's after)
    Incomprehensible filter and map. Map, splits

    Args:
        proc_id: id of process
        counts_filenames: filename of dictionaries to be loaded
        valid_subreddits: set of subreddits to be considered.
        valid_users: set of users to be considered.
        to_remove: set of users to be removed if this is a test set.

    """
    categories = set_to_dict(valid_subreddits)
    users = set_to_dict(valid_users)
    R, C = len(users), len(categories)
    result_data = np.zeros((0, 3))
    for filename in counts_filenames:
        print proc_id, filename
        counts = load_pickle(filename, False).items()
        sys.stdout.flush()
        points = filter(lambda x: x[0] in valid_users and x[1] in valid_subreddits,
                        map(lambda x: (x[0].split(' ')[0], x[0].split(' ')[1], int(x[1])), counts))
        data = np.zeros((len(points), 3))

        for i, p in enumerate(points):
            data[i] = [users[p[0]], categories[p[1]], p[2]]

        # save the partial array for downweighting later
        if to_remove is not None:
            for u in to_remove:
                mask = np.where(data[:, 0] == u)[0]
                data[mask, 2] = 0
        data = np.vstack((data, np.array([R - 1, C - 1, 0])))
        save_filename = filename.replace('uc_dict.pkl', 'UxS_%d.npy' % len(valid_users))
        save_array(save_filename, data, False)
        print proc_id, len(data)
        result_data = np.vstack((result_data, data))
    sys.stdout.flush()

    return result_data


if __name__ == '__main__':
    pass

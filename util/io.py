import os
import sys
import time

import cPickle as pickle
import numpy as np


def make_go_rw(filename, change_perm=True):
    if change_perm:
        os.chmod(filename, 0770)


def file_exists(filename):
    return os.path.isfile(filename)


def make_dir(filename):
    dir_path = os.path.dirname(filename)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


def build_path(dir, filename):
    return os.path.join(dir, filename)


def extract_filename(path):
    return os.path.split(path)[1]


def save_pickle(filename, obj, verbose=False, other_permission=True):
    make_dir(filename)
    if verbose:
        print '--> Saving ', filename, ' with pickle was ',
        sys.stdout.flush()
    t = time.time()
    with open(filename, 'wb') as gfp:
        pickle.dump(obj, gfp, protocol=pickle.HIGHEST_PROTOCOL)
        gfp.close()

    if verbose:
        print '%.3f s' % (time.time() - t)
    make_go_rw(filename, other_permission)


def load_pickle(filename, verbose=False):
    if verbose:
        print '--> Loading ', filename, ' with pickle was ',
        sys.stdout.flush()
    t = time.time()
    with open(filename, 'rb') as gfp:
        r = pickle.load(gfp)

    if verbose:
        print '%.3f s' % (time.time() - t)
    return r


def save_txt(filename, obj, delimiter=',', fmt='% .4e', verbose=True, other_permission=True):
    make_dir(filename)
    if verbose:
        print '--> Saving ', filename, ' with np.savetxt was ',
    sys.stdout.flush()
    t = time.time()
    np.savetxt(filename, obj, delimiter=delimiter, fmt=fmt)
    if verbose:
        print '%.3f s' % (time.time() - t)
    make_go_rw(filename, other_permission)


def save_text_sentences(filename, sentences, delimiter='\n', verbose=True, other_permission=True):
    make_dir(filename)
    t = time.time()
    if verbose:
        print '--> Saving ', filename, ' as a text file was ',
    sys.stdout.flush()
    with open(filename, 'w') as f:
        for s in sentences:
            f.write('%s%s' % (s, delimiter))
        f.close()
    if verbose:
        print '%.3f s' % (time.time() - t)
    make_go_rw(filename, other_permission)


def save_array(filename, obj, verbose=True, other_permission=True):
    filename = filename.replace('.pkl', 'npy')
    make_dir(filename)
    if verbose:
        print '--> Saving ', filename, ' with np.array was ',
    sys.stdout.flush()
    t = time.time()
    if not isinstance(obj, np.ndarray):
        obj = np.array(obj)
    np.save(filename, obj)
    if verbose:
        print '%.3f s' % (time.time() - t)
    make_go_rw(filename, other_permission)


def load_array(filename, verbose=True):
    if verbose:
        print '--> Loading ', filename, ' with np.load was ',
    filename = filename.replace('.pkl', 'npz')  # in case it exists
    sys.stdout.flush()
    t = time.time()
    r = np.load(filename)
    if verbose:
        print '%.3f s' % (time.time() - t)
    return r

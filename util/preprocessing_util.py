from scipy.sparse import coo_matrix


def set_to_dict(s, start=0):
    return dict(zip(sorted(list(s)), range(start, len(s) + start)))


def invert_dict(d):
    """Inverts keys, values in dict, it must have unique elements."""
    return dict([(v, k) for (k, v) in d.items()])


def combine_dicts(dict_list, init_value=0):
    """Combines dicitonaries by adding the values"""
    main_dict = {}
    for d in dict_list:
        for (k, v) in d.iteritems():
            main_dict[k] = main_dict.get(k, init_value) + v

    return main_dict


def data_to_sparse(data, shape=None, csc=False):
    """Takes in (n,3) array of data and returns csr matrix for that"""
    if csc:
        data_matrix = coo_matrix((data[:, 2], (data[:, 0].astype(int), data[:, 1].astype(int))), shape=shape).tocsc()
    else:
        data_matrix = coo_matrix((data[:, 2], (data[:, 0].astype(int), data[:, 1].astype(int))), shape=shape).tocsr()

    data_matrix.eliminate_zeros()
    return data_matrix


def sparse_to_data_array(matrix, dtype=np.float32, maintain_size=True):
    """Converts a sparse matrix into a data array (essentially a COO matrix)."""
    matrix.eliminate_zeros()
    data = np.zeros((matrix.nnz, 3), dtype=dtype)
    data[:, 0] = matrix.nonzero()[0]
    data[:, 1] = matrix.nonzero()[1]
    data[:, 2] = matrix.data
    if maintain_size:
        m, n = matrix.shape
        data = np.vstack((data, [m - 1, n - 1, 0]))  # maintain the size
    return data


def is_valid_entry():
    """You should write this, however you would like to define it!
    """

    return True

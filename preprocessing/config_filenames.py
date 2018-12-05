import os

project_dir = ''  # set this
n_proc = 16  # set number of processes to run concurrently

data_dir = os.path.join(project_dir, 'data')
valid_dir = os.path.join(data_dir, 'valid')
user_cat_dir = os.path.join(data_dir, 'user_category')


# -------------------------------------------
# general
# -------------------------------------------

def get_run_name(params, is_test=True):
    name = '_%d_%d_%d.pkl' % (params.min_subscribers, params.min_posts, params.vocab_size)

    if params.first_level:
        name = 'fl_%s' % name

    if is_test:
        if params.validation:
            name = name.replace('.pkl', '_validation.pkl')
        elif params.test:
            name = name.replace('.pkl', '_test.pkl')

    return name


def get_run_name_two(params):
    return '_%d_%d.pkl' % (params.min_subscribers, params.min_posts)


def get_fl_str(params):
    first_level = params.first_level

    fl_str = ''
    if first_level:
        fl_str = '_fl'
    return fl_str


def get_year_str(years):
    if years is None:
        return 'all'
    return '_'.join(map(str, years))


# -------------------------------------------
# subreddit popularity
# -------------------------------------------

def get_valid_sub_name(min_subscribers):
    return os.path.join(valid_dir, 'valid_subreddits_%d.pkl' % min_subscribers)


def get_sub_dict_name(subreddit_limit):
    return os.path.join(valid_dir, 'subscribers_dict_%d.pkl' % subreddit_limit)


# -------------------------------------------
# create valid users
# -------------------------------------------


def get_valid_user_filename(params, years):
    name = 'valid_users_%s_%s%s' % (get_year_str(years), get_fl_str(params), get_run_name_two(params))
    return os.path.join(valid_dir, name)


def get_known_bots_filename(min_subscribers):
    name = 'known_bots_%d.pkl' % min_subscribers
    return os.path.join(valid_dir, name)


def get_to_remove_users_filename(params):
    name = 'to_remove%s' % get_run_name(params, False)
    return os.path.join(valid_dir, name)


def get_user_activity_filename(user_num):
    name = 'user_active_months_%d.pkl' % user_num
    return os.path.join(data_dir, 'user_info', name)


# -------------------------------------------
# user category
# -------------------------------------------


def get_all_uc_dict_filenames(params, years=None):
    def is_valid_uc_dict_name(name, min_subscribers, fl_str, years=None):
        if years is None:
            return name.endswith('_%d%s_uc_dict.pkl' % (min_subscribers, fl_str))
        else:
            for y in years:
                if name.endswith('_%d%s_uc_dict.pkl' % (min_subscribers, fl_str)) and str(y) in name:
                    return True

        return False

    dir_name = user_cat_dir
    if params.validation:
        dir_name = os.path.join(dir_name, 'validation/')
    elif params.test:
        dir_name = os.path.join(dir_name, 'test/')

    files = os.listdir(dir_name)
    dict_filenames = []
    fl_str = get_fl_str(params)
    for filename in files:
        if is_valid_uc_dict_name(filename, params.min_subscribers, fl_str, years):
            dict_filenames.append(os.path.join(dir_name, filename))
    return sorted(dict_filenames)


def get_all_user_dict_filenames(params, years=None):
    """Returns filenames of training user_dicts"""

    def is_valid_user_dict_name(name, min_subscribers, fl_str, years=None):
        if years is None:
            return name.endswith('_%d%s_users_dict.pkl' % (min_subscribers, fl_str))
        else:
            for y in years:
                if name.endswith('_%d%s_users_dict.pkl' % (min_subscribers, fl_str)) and str(y) in name:
                    return True

        return False

    dir_name = user_cat_dir
    if params.validation:
        dir_name = os.path.join(dir_name, 'validation/')
    elif params.test:
        dir_name = os.path.join(dir_name, 'test/')

    files = os.listdir(dir_name)
    dict_filenames = []
    fl_str = get_fl_str(params)
    for filename in files:
        if is_valid_user_dict_name(filename, params.min_subscribers, fl_str, years):
            dict_filenames.append(os.path.join(user_cat_dir, filename))
    return dict_filenames

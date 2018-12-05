# __author__ = 'dimitrios'


class Parameters(object):
    min_subscribers = 50000
    min_posts = 100
    vocab_size = 25000
    h_index_min = 10
    first_level = False
    validation = False
    test = False

    def print_params(self):
        print '-' * 100
        print '|   Subscribers\t Min Posts\tVocab   h_idx\t First Only \t Val  \t Test\t|'
        print '|\t%d\t %d\t\t%d\t%d\t %s\t\t%s\t%s\t|' % (
            self.min_subscribers, self.min_posts, self.vocab_size, self.h_index_min, self.first_level, self.validation,
            self.test)
        print '-' * 100
        print

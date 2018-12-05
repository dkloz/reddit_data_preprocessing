import re

from nltk.tokenize import sent_tokenize, word_tokenize

not_existent_str = '___;;___;;__;;__1234567890234567823456789wertyui2345678dfghj45678sdcfvbnjkop;porywetxo3jpotcr;;;;^^'


def simplify_post(text):
    # remove markdown
    text = text.replace('*', '')
    text = text.replace('^', '')
    text = text.replace('>', '')
    text = text.replace('<', '')
    text = text.replace('#####', '')
    text = text.replace('####', '')
    text = text.replace('###', '')
    text = text.replace('##', '')
    text = text.replace('~~', '')
    text = text.replace('...', not_existent_str)
    text = text.replace('.. ', ' .. ')
    text = re.sub('\.\.\s', ' .. ', text)
    text = text.replace(not_existent_str, '...')

    # remove quotes, punctuation marks etc
    text = text.replace('``', '')
    text = text.replace('--', '')
    text = text.replace("''", '')

    # remove whitespaces.
    text = re.sub('\t+', ' ', text)
    text = re.sub('\n+', '\n', text)
    text = text.replace('.\n', '\n')
    text = text.replace('\n', '. ')
    text = text.replace('\r', '. ')
    text = text.replace(':.', ': ')
    return text


def entry_to_tokens(entry):
    """Takes in an entry as defined in the json raw files and returns a list of words. (encoded in utf-8 for saving)."""
    words = tokenize_words(simplify_post(entry['body']))
    try:
        words_str = [str(w.encode('utf-8')) for w in words]
    except Exception as e:
        print 'Exception converting to string: %s' % words
        print e.message
        return None
    return words_str


def tokenize_words(text):
    """Takes in a text string and returns a list of tokens (words). This keeps punctuation etc as separate tokens."""
    try:
        words = word_tokenize(text)
    except Exception as e:
        print 'Error in word_tokenize (from nltk)'
        print text
        print e.message
    return words


def tokenize_sentences(text):
    """A method that splits a text into multiple sentences. """
    return sent_tokenize(text)


def tokenize_sent_words(text):
    """Tokenizes a text in sentences and then words. Returns a list of lists. Each sentence is a list of words."""
    return [tokenize_words(s) for s in tokenize_sentences(text)]


def replace_with_ids(tokens, vocab):
    """Takes in a list of tokens and returns a list of word ids. <unk> must be included in vocabulary!

    Args:
        tokens: List of tokens.
        vocab: A dictionary from word -> word_id. MUST INCLUDE THE TERM <unk>.
    Returns:
        List of word ids.
    """
    return [vocab.get(t, vocab.get('<unk>')) for t in tokens]

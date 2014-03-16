from bs4 import BeautifulSoup as soup
import re
import urllib2

def strip_whitespace(ngram):
    """Strips whitespace from both ends, as well as redundant space in
    the middle of an ngram.
    """
    return ' '.join(re.split('[ \n]+', ngram.strip()))

def index_pattern(pat, li):
    """
    Returns a list of indices of occurrences of a pattern in a list of
    strings.
    """
    return [i for i, match in enumerate(li) if re.search(re.compile(pat), match)]

def shingle_print(text):
    '''Compute a shingle print based on tokens in text for deduping.'''
    return '#'.join([token for token in strip_whitespace(text.lower()).split(' ')\
                            if not(is_stopword(token))])

def test_shingle_print():
    # duplicate shingle prints
    # dupes go through
    pass

def is_stopword(token):
    return token in set(['is', 'the', 'of', 'a', 'at', 'in', 'to', 'an', 'on',\
                        'by'])

def get_title(url):
    """
    Given a url, this method returns the title of the URL.
    """
    page = soup(urllib2.urlopen(url))
    title = page.find('title')
    if title is None:
        title = page.find('TITLE')
    if title is not None:
        return str(title.text)
    else:
        return ''

def get_body(**kwargs):
    """Given a url, this method returns the body of the URL. Also permits
    argument to be a string containing HTML tags.
    @param 'url'
    @param 'html'
    """
    from boilerpipe.extract import Extractor
    try:
        if kwargs.get('url'):
            e = Extractor(extractor = 'ArticleExtractor', url = kwargs['url'])
            return e.getText()
        elif kwargs.get('html'):
            e = Extractor(extractor = 'ArticleExtractor', html = kwargs['html'])
            return e.getText()
        else:
            print 'Syntax: get_body([url/html] = argument)'
            raise Exception()
    except urllib2.HTTPError, e:
        #possibly a 404?
        print e.message
        return ''
    except UnicodeDecodeError, e:
        print e.message
        return ''

def get_sample_file(filename, percent = 10, output = ''):
    """
    Takes a file, and by default returns a sample with 10% of the lines.
    By default, prints the lines out.
    """
    f = open(filename)
    lines = f.readlines()
    smpsize = int(float(len(lines)) * float(percent) / 100)
    from random import sample
    indices = sample(xrange(len(lines)), smpsize)
    smp = []
    for i in indices:
        smp.append(lines[i].strip())
    if output:
        of = open(output, 'w')
        for ln in smp:
            of.write(ln.strip() + '\n')
        of.flush()
        of.close()
    return smp

def get_sentences(text):
    """Returns sentences in a chunk of text. Does so using the following pattern
    borrowed from:
    http://stackoverflow.com/questions/5553410/regular-expression-match-a-sentence
    """
    text_nonewlines = re.sub('\n', ' ', text)
    pat = '[^.!?\s][^.!?]*(?:[.!?](?![\'\"]?\s|$)[^.!?]*)*[.!?]?[\'\"]?(?=\s|$)'
    return re.findall(pat, text_nonewlines)

def is_number(word):
    """Recognizes if a given word contains a numerical value
    """
    return re.findall('\d*\.?\d+', word) != []

def replace_line(line1, line2, filename):
    fr = [l.strip() for l in open(filename).readlines()]
    fw = open(filename, 'w')
    pos = fr.index(line1)
    if pos:
        fr.insert(pos, line2)
        fr.remove(line1)
    fw.write('\n'.join(fr))
    fw.close()

def remove_line(line, filename):
    fr = [l.strip() for l in open(filename).readlines()]
    fw = open(filename, 'w')
    fr.remove(line)
    fw.write('\n'.join(fr))
    fw.close()

def tokenize(filename, simple = True, lowercase = True, stripchars = ';",.:| '):
    """Returns a collection of tokens from a file.

    :param filename: name of file to get tokens from
    :param simple: if False, use NLTK. See for e.g.: http://bit.ly/T2SEL8
    :param lowercase: returns tokens in lowercase
    :param stripchars: characters to remove from lead/trail of a token
    :returns: list of tokens
    """
    fr = open(filename)
    lines = [l.strip() for l in fr.readlines()]
    return __tokenize_lines(lines, simple, lowercase, stripchars)

def tokenize_text(text, simple = True, lowercase = True, stripchars = ';",.:| '):
    """Returns a collection of tokens from a string.

    :param text: one big blob of text
    :returns: list of tokens
    """
    lines = [l.strip() for l in text.split('\n')]
    return __tokenize_lines(lines, simple, lowercase, stripchars)
    

def __tokenize_lines(lines, simple = True, lowercase = True, stripchars = ';",.:|?!()- '):
    """Returns a collection of tokens from a list of lines

    :param lines: list of lines
    """
    import itertools
    
    tokens = []
    if simple:
        tokens = list(itertools.chain.from_iterable([l.split(' ')\
                                                     for l in lines]))
    else:
        tokens = []
    tokens = [t for t in tokens if t != '']
    if lowercase:
        return [w.strip(stripchars).lower() for w in tokens]
    else:
        return [w.strip(stripchars) for w in tokens]

def filter_stopwords(tokens, stopfile = '/home/shankar/work/box/projects/python/stopwords-en.txt'):
    """Takes a list of tokens and filters out stopwords.

    :param tokens: list of tokens
    :param stopfile: list of stopwords to use
    :returns: list of tokens with stopwords removed
    """
    stopwords = set([l.strip() for l in open(stopfile).readlines()])
    return [w for w in tokens if w.lower() not in stopwords]

def get_ngrams(text, n = 2, simple = True, lowercase = True, filterstopwords = True):
    """Returns a list of bigrams from the text of a file.

    :param text: Body of content
    :n: if = 2, bigrams, if 3, trigrams etc.
    :simple: If no fancy scoring is being done
    :lowercase: If bigrams are all returned in lowercase
    """
    # First get all tokens
    words = tokenize_text(text, lowercase=lowercase)
    if filterstopwords:
        words = filter_stopwords(words)
    ngrams = [' '.join([words[i+j] for j in range(n)]).strip()\
               for i in range(len(words)-n+1)]
    return ngrams

def get_capitalized_entities(sentence):
    '''Returns all capitalized entities from a string purported to be a 
    sentence.
    '''
    # cp = '([A-Z][A-Za-z]+( [A-Z][A-Za-z]+){1,5})'
    # cp = '([A-Z][A-Za-z]+( ([A-Z][A-Za-z]+|of|the|to)){1,5})'
    cp = '([A-Z][A-Za-z]+( (of |the |to )?([A-Z][A-Za-z]+)){1,5})'
    cre = re.compile(cp)
    entities = [i[0] for i in cre.findall(sentence) if cre.findall(sentence)]
    return entities

def get_sample_large_file(filename, sample = 10, output = ''):
    """Takes a large file, and by default returns a sample with 10% of the lines.
    By default, prints the lines out.
    """
    f = open(filename)
    of = ''
    if output != '':
        of = open(output, 'w')
    from random import random
    for line in f:
        if random() <= float(sample)/100:
            if of == '':
                print line.strip()
            else:
                of.write(line)

def compute_jaccard_similarity(line1, line2):
    """
    Computes Jaccard similarity between two lines. This is given by
    js = |words(line1) \cap words(line2)|/|words(line1) \cup words(line2)|
    """    
    words1 = set(line1.split(' '))
    words2 = set(line2.split(' '))

    return float(len(words1.intersection(words2)))/len(words1.union(words2))
    
def compute_similarity(line1, line2, method = 'jaccard'):
    """
    Computes the similarity between two strings using the method. The method
    can be one of ['cosine', 'jaccard']. Right now, returns Jaccard measure
    regardless of which method was chosen.

    Returns the computed distance/score as a value in [0, 1].
    """
    return compute_jaccard_similarity(line1, line2)

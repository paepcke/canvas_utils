'''
Created on Dec 8, 2018

@author: paepcke
'''
import io
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.tokenize import TreebankWordTokenizer

class TextPreprocessor(object):
    '''
    classdocs
    '''


    def __init__(self, text_file):
        '''
        Constructor
        '''
        self.frequencies = {}
        
        self.fill_frequencies(text_file)
        
    def fill_frequencies(self, text_file):
        try:
            stop_words = set(stopwords.words('english'))
        except Exception:
            nltk.download("stopwords")
            stop_words = set(stopwords.words('english'))
        path_pt = nltk.data.FileSystemPathPointer(text_file)
        data = nltk.data.load(path_pt, format='text')
        words = TreebankWordTokenizer().tokenize(data)
        # Remove stopwords
        words = [word for word in words if word not in stop_words]
                
        # Remove single-character tokens (mostly punctuation)
        words = [word for word in words if len(word) > 1]
        
        # Remove numbers
        words = [word for word in words if not word.isnumeric()]
        
        # Lowercase all words (default_stopwords are lowercase too)
        words = [word.lower() for word in words]
        
        # Calculate frequency distribution
        fdist = nltk.FreqDist(words)    

        print(words)
            
if __name__ == '__main__':
    file = '/Users/paepcke/tmp/april89.txt'
    tp = TextPreprocessor(file)
    print(tp)            
            
        
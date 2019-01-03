#!/usr/bin/env python
'''
Created on Dec 8, 2018

@author: paepcke
'''
import argparse
import os
import re
import sys

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import TreebankWordTokenizer


class TextPreprocessor(object):
    '''
    Inputs text either from stdin or from a file.
    Writes to stdout a CSV file headed 'Word,Frequency'.
    Each line contains one word from the text, and a 
    count of its occurrence. No sorting done.
    
    Text is tokenized, numbers and punctuation are removed,
    and all text is lower-cased before processing.
    '''
    domain_stopwords = [
                        'monday','tuesday','wednesday','thursday','friday',
                        'saturday','sunday','pm','this',"'s"
                        ]

    #--------------------------
    # Constructor 
    #----------------

    def __init__(self, text_file, has_attributes=False):
        '''
        Constructor
        '''
        self.frequencies = {}
        
        self.fill_frequencies(text_file, has_attributes)
    
    #--------------------------
    # fill_frequencies 
    #----------------
        
    def fill_frequencies(self, text_file, has_attributes=False):
        try:
            stop_words = set(stopwords.words('english'))
        except Exception:
            nltk.download("stopwords")
            stop_words = set(stopwords.words('english'))
            
        stop_words = stop_words.union(set(TextPreprocessor.domain_stopwords))
        # If text_file is None, we read text from stdin:
        if text_file is None:
            text_fd = sys.stdin
        else:
            text_fd = open(text_file, 'r')

        # Dict to store attribute ---> word list:
        words_by_course = {}
        if not has_attributes:
            # Collect all words from all topics under one key:
            words_by_course['all'] = []

        # Output the csv header:
        print('attribute,word,frequency')
        for (row_num, line) in enumerate(text_fd):
            if has_attributes:
                # Grab the attribute in a singleton array:
                attribute_arr = re.findall(r'([^,]*),', line)
                if len(attribute_arr) == 0:
                    #***sys.stderr.write("No attribute found in row %s" % row_num)
                    # Skip the row:
                    continue
                attr_key = attribute_arr[0]
                # Get line after the attribute and its trailing comma:
                line = line[len(attr_key)+1:]
                # Ensure this attribute as at least an empty word array
                # associated with it:
                if words_by_course.get(attr_key, None) is None:
                    words_by_course[attr_key] = []
            else:
                attr_key = 'all'
            
            words = TreebankWordTokenizer().tokenize(line)
            
            # Remove stopwords
            words = [word for word in words if word not in stop_words]
                    
            # Remove single-character tokens (mostly punctuation)
            words = [word for word in words if len(word) > 1]
            
            # Remove numbers
            words = [word for word in words if not word.isnumeric()]
            
            # Lowercase all words (default_stopwords are lowercase too)
            words = [word.lower() for word in words]

            # Merge words from this line with previously found words:
            words_by_course[attr_key].extend(words)
        
        # Calculate frequency distribution separately for each
        # attribute's word list:
        for (attr, words) in words_by_course.items():
            fdist = nltk.FreqDist(words)
            for word_freq_pair in fdist.items():
                sys.stdout.write('%s,%s,%s\n' % (attr,
                                                 word_freq_pair[0], 
                                                 word_freq_pair[1]))
               
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Creates frequency stats for text."
                                     )
    parser.add_argument('-i', '--infile',
                        help='If present, file with text to be processed. Default: read from stdin',
                        default=None)

    parser.add_argument('-a', '--attributes',
                        help='If present,  each line starts with a double-quoted attribute, ' +
                            'such as a course : "AA110",One text line.',
                        action='store_true',
                        default=False)

    args = parser.parse_args();
    
    text_file = args.infile
    # If file was specified, it must exist:
    if text_file is not None and not os.path.exists(text_file):
        print('File %s does not exist.' % text_file)
        
    #text_file = '/Users/paepcke/tmp/april89.txt'
    #text_file = None
    tp = TextPreprocessor(text_file, has_attributes=args.attributes)
    print(tp)            
            
        
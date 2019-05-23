#!/usr/bin/env python3

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse
import json


__author__ = ""
__email__ = ""

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

punctuations = string.punctuation

# You may need to write regular expressions.

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """


    # YOUR CODE GOES BELOW:
    text = re.sub('\s', ' ', text)
    # print(text)

    # remove links and subreddits
    text = re.sub(r'[(]?http\S+[)]?|][(].*?[)]| ?www\S+| ?\\', '', text)
    print("******** after removing links and subreddits**********")
    print(text)

    # split external punctuations and remove
    text = re.findall(r"\$\d+(?:,\d+)?(?:\w)?|\d+\.\d+|\w+(?:\.+\w+)|\w+(?:\;\w+)|\w(?:\.\w)|\w+(?:\.\”)|\w+(?:\-\w+)|\w+(?:\;\"\w)|\w+(?:\…)|\w+(?:\/\w+)(?:\/\w+)?(?:\w)|\w+(?:\(\w)|[\w'\u2014\’\“\”\@\🙄\👏\🇷🇺]+|[.!?,;:]",text)
    print('******** after spliting punctuation ***************')
    print(text)
    # convert uppercase to lower case
    text = [word.lower() for word in text]
    # print(text)

    # remove punctuation endings

    parsed_text = ''
    unigrams = ''
    bigrams = ''
    trigrams = ''
    for t in text:
        parsed_text += t+' '
        unigrams += t + ' ' if t not in punctuations else ''

    for i in range(len(text) -1):
        if text[i] not in punctuations and text[i+1] not in punctuations:
            bigrams += text[i] + '_' + text[i+1] + ' '

    for i in range(len(text)-2):
        if text[i] not in punctuations and text[i+1] not in punctuations and text[i+2] not in punctuations:
            trigrams += text[i] + '_' + text[i+1] + '_' + text[i+2] + ' '

    # remove the last whitespace
    if len(parsed_text) > 0:
        parsed_text = parsed_text[:-1] if parsed_text[-1] == ' ' else parsed_text
    if len(unigrams) > 0:
        unigrams = unigrams[:-1] if unigrams[-1] == ' ' else unigrams
    if len(bigrams) > 0:
        bigrams = bigrams[:-1] if bigrams[-1] == ' ' else bigrams
    if len(trigrams) > 0:
        trigrams = trigrams[:-1] if trigrams[-1] == ' ' else trigrams

    return [parsed_text, unigrams, bigrams, trigrams]


if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.

    # We are "requiring" your write a main function so you can
    # debug your code. It will not be graded.
    parser = argparse.ArgumentParser(description='cleantext.py takes in a json file containing comments of users '
                                                 , formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('filename',
                        help='comment file in json format')
    args = parser.parse_args()
    input_fn = args.filename
    # print(input_fn)
    comments = []
    returned_list = []
    with open(input_fn) as json_file:
        lines = json_file.readlines()
        for line in lines:
            # print(line)
            data = json.loads(line)
            # print(data['body'])
            comments.append(data['body'])
            # print('**************************')
            returned_list.append(sanitize(data['body']))

    print(returned_list)
    print(sanitize('hahahawww.google.com. hahaha #&¥*100% % @500$&*'))
    print(sanitize('[Let](https://www.merriam-webster.com/dictionary/let) could mean loads of things'))
    print(sanitize("don\\'t"))

    #print(sanitize("I'm afraid I can't explain myself, sir. Because I am not myself, you see?"))
    # output_file = 'comments.txt'
    # with open(output_file, 'w') as ofile:
    #     for comment in comments:
    #         ofile.write(comment)

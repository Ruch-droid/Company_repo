# pylint: disable=no-member

import pandas as pd
from textblob import TextBlob

import textstat
import spacy
import nltk
from nltk.tokenize import word_tokenize

# Read the CSV file into a DataFrame
df = pd.read_csv("/home/ruchika/Downloads/test.csv", encoding="utf-8")

  
def tokenize_text(text):
    return nltk.word_tokenize(text)

# Tokenize the text in the DataFrame
df['tokenized_text'] = df['Text'].apply(tokenize_text)



# Data Preprocessing
df['Text'] = df['Text'].apply(lambda x: " ".join(x.lower() for x in x.split())) # Convert to lowercase
df['Text'] = df['Text'].str.replace('[^\w\s]','') # Remove punctuation
stop_words = set(nltk.corpus.stopwords.words('english'))
df['Text'] = df['Text'].apply(lambda x: " ".join(x for x in x.split() if x not in stop_words)) # Remove stopwords

# Sentiment Analysis
df['polarity'] = df['Text'].apply(lambda x: TextBlob(x).sentiment.polarity)
df['subjectivity'] = df['Text'].apply(lambda x: TextBlob(x).sentiment.subjectivity)

# Print the updated dataframe
print(df.head())





#df.to_csv("/home/ruchika/test1.csv")

import pandas as pd
import numpy as np
from transformers import pipeline

def clean_summary(df: pd.DataFrame) -> pd.DataFrame:    
    df['summary'] = df['summary'].str.replace(r'This are the title and description.*?\.', '', regex=True)
    df['summary'] = df['summary'].str.replace(r'Here you.*?\:', '', regex=True)
    df['summary'] = df['summary'].str.replace("(do not take in count urls and social media)", "")
    return df

def sentiment_analysis(df: pd.DataFrame) -> pd.DataFrame:
    sentiment_analyzer = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")

    def analyze_sentiment(text):
        result = sentiment_analyzer(text[:512])  # Limit text length for performance
        return result[0]['label'], result[0]['score']

    sentiments = df['comments'].apply(analyze_sentiment)
    df['sentiment_label'] = sentiments.apply(lambda x: x[0])
    
    return df
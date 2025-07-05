import random

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
import joblib

import pandas as pd

from time import time
import re

cleanup_re = re.compile('[^a-z]+')


def cleanup(sentence):
    sentence = sentence.lower()
    sentence = cleanup_re.sub(' ', sentence).strip()
    return sentence


if __name__ == "__main__":
    dataset_path = "deploy/resources/data/workload_data/lr/amzn_fine_food_reviews/reviews100mb.csv"
    model_file_path = "deploy/resources/data/workload_data/lr/model.pk"
    input_file = "deploy/resources/data/workload_data/lr/input.txt"
    with open(input_file, 'r') as f:
        lines = f.readlines()
        inputs = [line.strip() for line in lines if line.strip()]
    df = pd.read_csv(dataset_path)
    start = time()
    df['train'] = df['Text'].apply(cleanup)
    tfidf_vect = TfidfVectorizer(min_df=100).fit(df['train'])
    for x in inputs:
        df_input = pd.DataFrame()
        df_input['x'] = [x]
        X = tfidf_vect.transform(df_input['x'])
        train = tfidf_vect.transform(df['train'])
        model = joblib.load(model_file_path)
        y = model.predict(X)

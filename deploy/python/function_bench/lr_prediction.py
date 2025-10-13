import random
import sys

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
    input_file = "workload_data/lr/input.txt"
    with open(input_file, 'r') as f:
        lines = f.readlines()
        inputs = [line.strip() for line in lines if line.strip()]

    mode = sys.argv[1]
    if mode not in ['long', 'short', 'medium']:
        raise ValueError("Invalid mode. Use 'long' or 'short'.")
    if mode == 'long':
        dataset_path = "workload_data/lr/train_data/reviews100mb.csv"
        model_file_path = "workload_data/lr/model_large.pk"
    elif mode == 'medium':
        dataset_path = "workload_data/lr/train_data/reviews50mb.csv"
        model_file_path = "workload_data/lr/model_medium.pk"
    else:  # mode == 'short'
        dataset_path = "workload_data/lr/train_data/reviews10mb.csv"
        model_file_path = "workload_data/lr/model_small.pk"
    df = pd.read_csv(dataset_path)
    df['train'] = df['Text'].apply(cleanup)
    tfidf_vect = TfidfVectorizer(min_df=100).fit(df['train'])
    for x in inputs:
        df_input = pd.DataFrame()
        df_input['x'] = [x]
        X = tfidf_vect.transform(df_input['x'])
        model = joblib.load(model_file_path)
        y = model.predict(X)

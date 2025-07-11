import random
import sys

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression

import pandas as pd

from time import time
import re

cleanup_re = re.compile('[^a-z]+')


def cleanup(sentence):
    sentence = sentence.lower()
    sentence = cleanup_re.sub(' ', sentence).strip()
    return sentence


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode not in ['long', 'short', 'medium']:
        raise ValueError("Invalid mode. Use 'long' or 'short'.")
    if mode == 'long':
        dataset_sizes = [10, 20, 50, 100]
    elif mode == 'medium':
        dataset_sizes = [50]
    else:  # mode == 'short'
        dataset_sizes = [10]
    for dataset_size in dataset_sizes:
        file_name = f"reviews{dataset_size}mb.csv"
        dataset_path = "workload_data/lr/train_data/" + file_name
        df = pd.read_csv(dataset_path)
        start = time()
        df['train'] = df['Text'].apply(cleanup)
        tfidf_vect = TfidfVectorizer(min_df=100).fit(df['train'])
        train = tfidf_vect.transform(df['train'])
        model = LogisticRegression(max_iter=1000)
        model.fit(train, df['Score'])
        latency = time() - start
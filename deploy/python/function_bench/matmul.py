import random

import numpy as np
from time import time

def matmul(n):
    A = np.random.rand(n, n)
    B = np.random.rand(n, n)

    start = time()
    C = np.matmul(A, B)
    latency = time() - start

    return latency

if __name__ == "__main__":
    N = [1024, 2048, 4096, 8192]  # Adjust N as needed for testing
    for n in N:
        latency = matmul(n)
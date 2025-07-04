import random

import numpy as np
from time import time

def matmul(N):
    A = np.random.rand(N, N)
    B = np.random.rand(N, N)

    start = time()
    C = np.matmul(A, B)
    latency = time() - start

    return latency

if __name__ == "__main__":
    N = random.randint(4094, 8192)  # Adjust N as needed for testing
    latency = matmul(N)
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
    import sys
    mode = sys.argv[1]
    if mode not in ['long', 'short', 'medium']:
        raise ValueError("Invalid mode. Use 'long' or 'short'.")
    if mode == 'long':
        N = [1024, 2048, 4096, 8192]  # Adjust N as needed for testing
    elif mode == 'medium':
        N = [1024, 2048]
    else:  # mode == 'short'
        N = [256, 512]
    for n in N:
        latency = matmul(n)
import math
import sys


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == 'long':
        N = 10000000
    elif mode == 'medium':
        N = 1000000
    elif mode == 'short':
        N = 100000
    else:
        raise ValueError("Invalid mode. Use 'long' or 'short'.")
    N = 10000000
    for i in range(0, N):
        sin_i = math.sin(i)
        cos_i = math.cos(i)
        sqrt_i = math.sqrt(i)

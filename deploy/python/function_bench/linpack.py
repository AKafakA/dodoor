from numpy import matrix, array, linalg, random, amax
from time import time


def linpack(N):
    eps = 2.22e-16

    ops = (2.0 * N) * N * N / 3.0 + (2.0 * N) * N

    # Create AxA array of random numbers -0.5 to 0.5
    A = random.random_sample((N, N)) - 0.5
    B = A.sum(axis=1)

    # Convert to matrices
    A = matrix(A)

    B = matrix(B.reshape((N, 1)))
    na = amax(abs(A.A))

    start = time()
    X = linalg.solve(A, B)
    latency = time() - start

    mflops = (ops * 1e-6 / latency)

    result = {
        'mflops': mflops,
        'latency': latency
    }

    return result


if __name__ == "__main__":
    import sys
    mode = sys.argv[1]
    if mode == 'long':
        N = 4096
    elif mode == 'medium':
        N = 2048
    elif mode == 'short':
        N = 256
    else:
        raise ValueError("Invalid mode. Use 'long' or 'short'.")
    result = linpack(N)

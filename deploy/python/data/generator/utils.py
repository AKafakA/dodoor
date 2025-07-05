import numpy as np

def get_wait_time(qps: float, distribution: str, burstiness: float = 1.0) -> float:
    mean_time_between_requests = 1.0 / qps
    if distribution == "uniform":
        return mean_time_between_requests
    elif distribution == "gamma":
        assert burstiness > 0, (
            f"A positive burstiness factor is expected, but given {burstiness}.")
        theta = 1.0 / (qps * burstiness)
        return np.random.gamma(shape=burstiness, scale=theta)
    else:
        return np.random.exponential(mean_time_between_requests)
import numpy as np

def add_laplace_noise(data, sensitivity, epsilon):
    noise = np.random.laplace(0, sensitivity / epsilon, len(data))
    return data + noise

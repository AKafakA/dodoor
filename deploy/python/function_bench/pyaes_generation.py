from time import time
import random
import string
import pyaes


def generate(length):
    letters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(letters) for i in range(length))


if __name__ == "__main__":
    import sys
    mode = sys.argv[1]
    if mode not in ['long', 'short', 'medium']:
        raise ValueError("Invalid mode. Use 'long' or 'short'.")
    if mode == 'long':
        length_of_message = [1024, 2048, 4096, 8192]  # Adjust length as needed
        num_of_iterations = 100  # Number of iterations for each length
    elif mode == 'medium':
        length_of_message = [1024, 2048]
        num_of_iterations = 10
    else:  # mode == 'short'
        length_of_message = [1024]
        num_of_iterations = 5
    KEY = b'\xa1\xf6%\x8c\x87}_\xcd\x89dHE8\xbf\xc9,'
    for length in length_of_message:
        message = generate(length)
        start = time()
        for loops in range(num_of_iterations):
            aes = pyaes.AESModeOfOperationCTR(KEY)
            ciphertext = aes.encrypt(message)

            aes = pyaes.AESModeOfOperationCTR(KEY)
            plaintext = aes.decrypt(ciphertext)
            aes = None

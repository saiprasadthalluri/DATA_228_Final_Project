from random import getrandbits

def trailing_zeroes(n):
    return len(bin(n)) - len(bin(n).rstrip('0'))

def flajolet_martin(stream):
    max_zeroes = 0
    for item in stream:
        hash_value = getrandbits(32)
        max_zeroes = max(max_zeroes, trailing_zeroes(hash_value))
    return 2 ** max_zeroes

from pybloom_live import BloomFilter

bloom = BloomFilter(capacity=1000, error_rate=0.001)
bloom.add("bitcoin")

if "bitcoin" in bloom:
    print("Exists!")

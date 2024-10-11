class DGIM:
    def __init__(self, window_size):
        self.window_size = window_size
        self.buckets = []

    def add_bit(self, bit):
        if bit == 1:
            self.buckets.append((1, len(self.buckets) + 1))
        while len(self.buckets) > 1 and self.buckets[-1][1] - self.buckets[0][1] > self.window_size:
            self.buckets.pop(0)

    def count_ones(self):
        if not self.buckets:
            return 0
        return sum(b[0] for b in self.buckets[:-1]) + self.buckets[-1][0] // 2

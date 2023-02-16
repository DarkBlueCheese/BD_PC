import pandas
from mrjob.job import MRJob
from mrjob.step import MRStep

import csv


class NGrammer(MRJob):
    
    def mapper1(self, _, line):
        reader = line.split('\t')
        for row in [reader]:
            yield (row[0], (int(row[2]), int(row[4])))

    def reducer1(self, gram_word, gram_counts_books):
        gram_counts_sum = 0
        gram_books_sum = 0

        for c, b in gram_counts_books:
            gram_counts_sum += c
            gram_books_sum += b

        yield gram_word, (gram_counts_sum / gram_books_sum)

    def steps(self):
        return [MRStep(mapper = self.mapper1, reducer = self.reducer1),]


if __name__ == '__main__':
    NGrammer.run()
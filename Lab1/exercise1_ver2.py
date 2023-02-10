import pandas
from mrjob.job import MRJob
from mrjob.step import MRStep

import csv

# 1) n-gram itself
# 2) year in which the n-gram appeared
# 3) number of times the n-gram appeared in the books from the corresponding year (count) 
# 4) number of pages on which the n-gram appeared in this year (page-count)
# 5) number of distinct books in which the n-gram appeared in this year (book count)

class CSVMapper(MRJob):
    
    def mapper1(self, _, line):
        # reader = csv.reader([line], delimiter='\t')
        reader = line.split('\t')
        for row in [reader]:
            yield (row[0], int(row[2]))
    
    # 32968   "Protein synthesis"

    # def reducer1(self, key, values):
    #     yield key, sum(values)

    def reducer1(self, key, values):
        yield (key, sum(values))

###
    def reducer2(self, key, values):
        yield (None, max(values, key))

    def steps(self):
        return [MRStep(mapper = self.mapper1, reducer = self.reducer1),
                MRStep(reducer = self.reducer2)]
###

    # def reducer1(self, key, values):
    #     yield(None, (sum(values[])))

if __name__ == '__main__':
    CSVMapper.run()
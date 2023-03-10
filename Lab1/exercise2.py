import pandas
from mrjob.job import MRJob
from mrjob.step import MRStep

import csv

# 1) n-gram itself
# 2) year in which the n-gram appeared
# 3) number of times the n-gram appeared in the books from the corresponding year (count) 
# 4) number of pages on which the n-gram appeared in this year (page-count)
# 5) number of distinct books in which the n-gram appeared in this year (book count)

class NGrammer(MRJob):
    
    def mapper1(self, _, line):
        reader = line.split('\t')
        for row in [reader]:
            yield ((row[0], row[1]), int(row[2])) 

    def reducer1(self, gram_year_list, gram_counts):
        yield (gram_year_list, (sum(gram_counts)))


    def mapper2(self, gram_year_list, gram_count_sums) :
        gram_word, gram_year = gram_year_list

        yield (gram_year, (gram_word, gram_count_sums)) 


###
    # def reducer2(self, gram_year, gram_word_sums):
    #     # gram_word, gram_year = gram_year_list
    #     # # gram_year_count = (gram_word, gram_year, gram_counts_sum)
    #     # year_count = (gram_year, gram_counts_sum)
    #     # cheater_value, cheater_key = max(gram_counts_sum)
    #     max_sums = 0
    #     max_word = None
    #     for word, sums in gram_word_sums:
    #         if sums > max_sums:
    #             max_sums = sums
    #             max_word = word

    #     # yield (None, max(gram_counts_sum, key = lambda x : x[1]))
    #     yield None, (gram_year, max_word, max_sums)


    def reducer2(self, gram_year, gram_word_sums):

        yield None, (gram_year, *max(gram_word_sums, key = lambda x : x[1]))



    def steps(self):
        return [MRStep(mapper = self.mapper1, reducer = self.reducer1),
                # MRStep(mapper = self.mapper2)]
                MRStep(mapper = self.mapper2, reducer = self.reducer2)]


if __name__ == '__main__':
    NGrammer.run()
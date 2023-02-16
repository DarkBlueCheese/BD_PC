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
            yield (row[0], (int(row[2]), int(row[4])))
            # yield ((row[0], int(row[2])), int(row[4]))
            # yield ((int(row[2]), int(row[4])), row[0])

    def reducer1(self, gram_word, gram_counts_books):
        gram_counts_sum = 0
        gram_books_sum = 0


        for c, b in gram_counts_books:
            gram_counts_sum += c
            gram_books_sum += b

        yield gram_word, (gram_counts_sum / gram_books_sum)


    # def mapper2(self, gram_book_list, gram_count_sums) :
    #     gram_word, gram_book = gram_book_list

    #     yield (gram_word, (sum(gram_count_sums), sum(gram_book)))





    # def reducer2(self, gram_word, gram_count_sums_books_sum):
        
    #     yield gram_word, (sum(gram_count_sums), sum(gram_book))



    def steps(self):
        return [MRStep(mapper = self.mapper1, reducer = self.reducer1),]
                # MRStep(reducer = self.reducer2)]

if __name__ == '__main__':
    NGrammer.run()
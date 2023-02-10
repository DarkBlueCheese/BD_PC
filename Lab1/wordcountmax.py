from mrjob.job import MRJob
from mrjob.step import MRStep

class MRmyjob(MRJob):
    def mapper1(self, _, line):
        wordlist = line.split()
        for word in wordlist:
            yield(word, 1)

    def reducer1(self, key, list_of_values):
        yield(None, (sum(list_of_values),key))

    def reducer2(self, _, list_of_values):
        yield(max(list_of_values))

    def steps(self):
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1), 
                MRStep(reducer=self.reducer2)]


if __name__ == '__main__':
    MRmyjob.run()


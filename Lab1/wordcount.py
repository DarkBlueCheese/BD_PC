from mrjob.job import MRJob

class MRmyjob(MRJob):
	def mapper(self, _, line):
		wordlist = line.split()
		for word in wordlist:
			yield word,1

	def reducer(self, key, list_of_values):
		yield key,sum(list_of_values)

if __name__ == '__main__':
    MRmyjob.run()





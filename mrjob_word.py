
## for "high": python mrjob_word.py high_stem.txt -o ./output
# cat ./output/part* > output_words_high.txt
## for "low": python mrjob_word.py low_stem.txt -o ./output
# cat ./output/part* > output_words_low.txt


from mrjob.job import MRJob
import re
import mrjob
import mrjob.protocol

WORD_RE = re.compile('[\w\']+')  # regular expression


class MRMostUsedWord(mrjob.job.MRJob):
    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for word in words:
            yield word.lower(), 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == "__main__":
    MRMostUsedWord.run()

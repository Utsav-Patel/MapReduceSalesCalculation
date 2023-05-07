# Time taken to complete MapReduce job:  0:00:08.892412
import csv
import calendar
import sys
sys.path.append("/Users/Dell/Documents/My Files/Rutgers/CS553 - Design of Internet Services/MapReduceSalesCalculation")

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
from constants import K, DATA_PATH


class MRItemCount(MRJob):

    def __init__(self, *args, **kwargs):

        super(MRItemCount, self).__init__(*args, **kwargs)
        self.csv_reader = None

    def mapper1_init(self):
        self.csv_reader = csv.reader(open(DATA_PATH))

    def mapper1(self, _, __):
        f = "%m/%d/%y %H:%M"
        for row in self.csv_reader:
            if row[2].isnumeric():
                info = datetime.strptime(row[4], f)
                month = calendar.month_abbr[info.month]
                yield str(month), int(row[2]) * float(row[3])

    def combiner1(self, month, revenue):
        yield month, sum(revenue)

    def reducer1(self, month, revenue):
        yield month, sum(revenue)

    def mapper2(self, month, revenue):
        yield None, (revenue, month)

    def reducer2(self, _, tuples):
        tuples = list(tuples)
        tuples.sort(reverse=True)

        for i in range(K):
            yield tuples[i][1], tuples[i][0]

    def steps(self):
        return [
            MRStep(mapper=self.mapper1, mapper_init=self.mapper1_init, combiner=self.combiner1, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2)
        ]


if __name__ == "__main__":
    start_time = datetime.now()
    MRItemCount.run()
    end_time = datetime.now()

    print('Time taken to complete MapReduce job: ', (end_time - start_time))

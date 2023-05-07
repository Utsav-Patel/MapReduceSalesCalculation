# Time taken to complete MapReduce job:  0:00:03.993000
import csv
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
        for row in self.csv_reader:
            if row[2].isnumeric():
                yield row[1], int(row[2])

    def reducer1(self, product, quantities):
        yield product, sum(quantities)

    def mapper2(self, product, quantity):
        yield None, (quantity, product)

    def reducer2(self, _, tuples):
        tuples = list(tuples)
        tuples.sort(reverse=True)
        for i in range(K):
            yield i+1, ('Product: ' + tuples[i][1] + ' Quantity: ' + str(tuples[i][0]))

    def steps(self):
        return [
            MRStep(mapper=self.mapper1, mapper_init=self.mapper1_init, reducer=self.reducer1, jobconf={
                       "mapred.reduce.tasks": K, "mapred.mapper.tasks": 1
                   }),
            MRStep(mapper=self.mapper2, reducer=self.reducer2)
        ]


if __name__ == "__main__":

    start_time = datetime.now()
    MRItemCount.run()
    end_time = datetime.now()

    print('Time taken to complete MapReduce job: ', (end_time - start_time))

import csv

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
from constants import K, DATA_PATH


class MRAnnualRevenue(MRJob):

    def __init__(self, *args, **kwargs):
        super(MRAnnualRevenue, self).__init__(*args, **kwargs)
        self.csv_reader = None

    def mapper1_init(self):
        self.csv_reader = csv.reader(open(DATA_PATH))

    def mapper1(self, _, __):
        product_revenue = dict()
        for row in self.csv_reader:
            if row[2].isnumeric():
                if row[1] not in product_revenue:
                    product_revenue[row[1]] = int(row[2]) * float(row[3])
                else:
                    product_revenue[row[1]] += int(row[2]) * float(row[3])
        for product in product_revenue:
            yield product, product_revenue[product]

    def reducer1(self, product, quantities):
        # assert len(list(quantities)) == 1
        yield product, list(quantities)

    def mapper2(self, product, quantity):
        yield None, (quantity, product)

    def reducer2(self, _, tuples):
        tuples = list(tuples)
        tuples.sort(reverse=True)
        for i in range(K):
            yield i+1, ('Product: ' + tuples[i][1] + ' Annual Revenue: ' + str(tuples[i][0]))

    def steps(self):
        return [
            MRStep(mapper=self.mapper1, mapper_init=self.mapper1_init, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2)
        ]


if __name__ == "__main__":

    start_time = datetime.now()
    MRAnnualRevenue.run()
    end_time = datetime.now()

    print('Time taken to complete MapReduce job: ', (end_time - start_time))

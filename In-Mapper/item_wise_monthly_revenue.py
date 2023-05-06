import csv
import calendar

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
from constants import K, DATA_PATH


class MRItemWiseMonthlyRevenue(MRJob):

    def __init__(self, *args, **kwargs):
        super(MRItemWiseMonthlyRevenue, self).__init__(*args, **kwargs)
        self.csv_reader = None

    def mapper1_init(self):
        self.csv_reader = csv.reader(open(DATA_PATH))

    def mapper1(self, _, __):
        month_product_to_revenue = dict()
        f = "%m/%d/%y %H:%M"
        for row in self.csv_reader:
            if row[2].isnumeric():
                info = datetime.strptime(row[4], f)
                month = calendar.month_abbr[info.month]
                if (str(month), str(row[1])) not in month_product_to_revenue:
                    month_product_to_revenue[(str(month), str(row[1]))] = int(row[2]) * float(row[3])
                else:
                    month_product_to_revenue[(str(month), str(row[1]))] += int(row[2]) * float(row[3])
        for month_product in month_product_to_revenue:
            yield month_product, month_product_to_revenue[month_product]

    def reducer1(self, month_product, revenue):
        yield month_product, sum(revenue)

    def mapper2(self, month_product, revenue):
        yield month_product[1], (revenue, month_product[0])

    def reducer2(self, product, tuples):
        tuples = list(tuples)
        tuples.sort(reverse=True)

        for i in range(K):
            yield product, (tuples[i][1], tuples[i][0])

    def steps(self):
        return [
            MRStep(mapper=self.mapper1, mapper_init=self.mapper1_init, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2)
        ]


if __name__ == "__main__":
    start_time = datetime.now()
    MRItemWiseMonthlyRevenue.run()
    end_time = datetime.now()

    print('Time taken to complete MapReduce job: ', (end_time - start_time))
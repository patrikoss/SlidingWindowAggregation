import unittest
from sliding_aggregation import calculate_window_sum, sc

class TestWindowAggregation(unittest.TestCase):

    def setUp(self):
        pass


    def run_test(self, partitions, window, data_in, expected_out):
        """
        :param partitions: int - number of partitions
        :param window: int - window lengtj
        :param data_in: [(number, weight), ...] - list of numbers along with their weights
        :param expected_out: [(number, window-sum)] - list of numbers along with their window-sum
        :return: true if the test passes
        """
        data_in = [str(el[0])+' ' + str(el[1]) for el in data_in]
        data_in = sc.parallelize(data_in)
        data_out = calculate_window_sum(data_in, window, partitions)
        data_out = data_out.map(lambda el: tuple((int(x) for x in el.split(" ")))).collect()

        expected_out = sorted(expected_out)
        expected_ranks_out = [(i, expected_out[i][1]) for i in range(len(expected_out)) ]

        self.assertEqual(sorted(data_out), expected_ranks_out, "Difference when window={}, partitions={}".format(window, partitions))

    def test_small(self):
        MIN_N, MAX_N = 17,18
        MIN_WINDOW, MAX_WINDOW = 3, 16
        MIN_PARTITIONS, MAX_PARTITIONS = 3, 8

        pref_sum = [i*(i+1)//2 for i in range(MAX_N+1)]
        for n in range(MIN_N, MAX_N+1):
            data_in = [(i, i) for i in range(n)]
            for window in range(MIN_WINDOW, MAX_WINDOW + 1):
                expected_out = [(i, pref_sum[i] - (pref_sum[i-window] if i-window >= 0 else 0) ) for i in range(n)]
                for partitions in range(MIN_PARTITIONS, MAX_PARTITIONS+1):
                    self.run_test(partitions, window, data_in, expected_out)


if __name__=='__main__':
    unittest.main()

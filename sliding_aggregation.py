import math
import random
from pyspark.sql.types import *
import pyspark
import sys

spark = pyspark.sql.SparkSession.builder.appName('window_sum').getOrCreate()
sc = spark.sparkContext

def target_machine_by_boundary(boundaries):
    def get_machine_number(number):
        for i in range(len(boundaries.value)):
            if number <= boundaries.value[i]:
                return i
        return len(boundaries.value)

    def get_machine_number_bisect(number):
        l, r = 0, len(boundaries.value) - 1
        while l < r:
            m = (l+r) // 2
            if number < boundaries.value[m]:
                r = m
            elif number == boundaries.value[m]:
                return m
            else: # number > boundaries[m]
                l = m+1
        return l

    return get_machine_number_bisect

def target_machine_by_rank(m):
    def get_machine_number(rank):
        return rank // m
    return get_machine_number

def swap(pair):
    return pair[1], pair[0]

def perfect_sorting(data, m, n, t):
    # choose sampling probability appropriately
    sampling_prob = (1 / m) * math.log(n * t)
    random.seed(1)

    # choose some samples from the data
    samples = data.filter(lambda pair: random.random() < sampling_prob)
    samples = sorted(samples.collect())

    # calculate boundaries between machines and broadcast them to all machines
    step = math.ceil(len(samples) / t)
    boundaries = [p[0] for p in samples[step::step]]
    boundaries = sc.broadcast(boundaries)

    # perfect sorting
    return data \
        .repartitionAndSortWithinPartitions(t, partitionFunc=target_machine_by_boundary(boundaries)) \
        .zipWithIndex() \
        .map(swap) \
        .repartitionAndSortWithinPartitions(t, partitionFunc=target_machine_by_rank(m))

def partition_sum(partition):
    yield sum(partition)


def partial_elements_sender(l, m, t):
    """Sends single elements to at most 3 machines(including own machine)"""
    def calculate(element):
        rank, weight = element
        element_machine = rank//m
        machines = set()
        machines.add(element_machine)
        if l <= m and element_machine+1 < t:
            machines.add(element_machine+1)
        else:
            first, second = element_machine + (l-1)//m, element_machine+1 + (l-1)//m
            if first < t:
                machines.add(first)
            if second < t:
                machines.add(second)
        return [(machine, element) for machine in list(machines)]
    return calculate


def elements_summer(m, l, partition_weights):

    def whole_partition_summer(partition_no):
        left_whole, right_whole = partition_no - (l-1)//m, partition_no - 1

        """Calculates the sum of partitions from a+1, a+2, ..., b-1"""
        if left_whole > right_whole:
            whole_sum = 0
        else:
            whole_sum = (partition_weights.value[right_whole] if right_whole >=0 else 0) - (partition_weights.value[left_whole] if left_whole >= 0 else 0)
        return whole_sum

    def elements_partition_summer(kv):
        """Generates a list of (rank, value) pairs where
        rank is a ranking of an element from a given partition
        value is the sum of l largerst elements not exceeding the element"""
        partition_no, elements = kv
        elements = sorted(elements)
        whole_sum = whole_partition_summer(partition_no)
        result = []
        left, right, partial_sum = 0, 0, 0
        for target_rank in range(partition_no*m, (partition_no+1)*m):
            while right < len(elements) and elements[right][0] <= target_rank:
                partial_sum += elements[right][1]
                right += 1
            while left < len(elements) and elements[left][0] + l - 1 < target_rank:
                partial_sum -= elements[left][1]
                left += 1
            result += (target_rank, partial_sum+whole_sum),
        return result

    return elements_partition_summer


def calculate_window_sum(data, window_size, number_of_partitions):

    data = data.repartition(number_of_partitions)

    l = window_size
    # number of numbers :)
    n = data.count()
    # number of machines
    t = data.getNumPartitions()

    # avg number of numbers to process on each machine
    m=math.ceil(n/t)
    # transform data from string to pair of integers
    data = data.map(lambda l: l.split()).map(lambda p: (int(p[0]), int(p[1])))

    # generate elements of the form (rank, (numner, weight) )
    sorted_data_with_number = perfect_sorting(data, m, n, t)

    sorted_data_with_number.cache()

    # eliminate the number from the elements, leaving only elements of the form (rank, weight)
    sorted_data = sorted_data_with_number.map(lambda t: (t[0], t[1][1]) )

    # calculate the prefix sum of weights for partitions
    partition_weights = sorted_data.map(lambda p: p[1]).mapPartitions(partition_sum).collect()
    for i in range(1, len(partition_weights)):
        partition_weights[i] += partition_weights[i-1]
    partition_weights = sc.broadcast(partition_weights)

    # send single elements to approprate machines
    partial_elements = sorted_data.flatMap(partial_elements_sender(l,m,t))

    # collect all the remotely-relevant elements along with local elements on appropriate machine
    partial_elements = partial_elements.groupByKey().mapValues(list)

    # calculate the window-sum for each element and associate the result with the rank of the element
    ranks_with_window = partial_elements.map(elements_summer(m,l,partition_weights)).flatMap(lambda x: x)

    ### OUTPUT OPTION1: output the result in the form: rank, window-sum
    ranks_with_window = ranks_with_window.filter(lambda x: x[0] < n).map(lambda x: str(x[0]) + ' ' + str(x[1]))
    ### END OPTION 1

    ### OUTPUT OPTION2: output the result in the form: number, window-sum
    #restore the original element based on the rank
    #elements_with_window = ranks_with_window.join(sorted_data_with_number)

    # store the result in the form of (number, window-sum) rather than (rank, (window-sum, (number, weight)))
    #numbers_with_window = elements_with_window.map(lambda p: (p[1][1][0], p[1][0]) ).sortByKey()

    # output result to file, output is of the form: (number, window-sum)
    #numbers_with_window.map(lambda x: str(x[0]) + ' ' + str(x[1]) )
    #END OPTION 2

    return ranks_with_window


def main(path_to_input, window_size, path_to_output, number_of_partitions):

    spark.conf.set("spark.sql.shuffle.partitions", number_of_partitions)
    spark.conf.set("spark.default.parallelism", number_of_partitions)

    data = sc.textFile(path_to_input)
    ranks_with_window = calculate_window_sum(data, window_size, number_of_partitions)
    ranks_with_window.saveAsTextFile(path_to_output)

if __name__=='__main__':
    if len(sys.argv) != 5:
        sys.stderr("Incorrect number of arguments")
    path_to_input = sys.argv[1]
    window_size = int(sys.argv[2])
    path_to_output = sys.argv[3]
    number_of_partitions = int(sys.argv[4])
    main(path_to_input, window_size, path_to_output, number_of_partitions)

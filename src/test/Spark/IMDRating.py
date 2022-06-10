from pyspark.sql.types import IntegerType,DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark import SparkContext
import sys
sc = SparkContext.getOrCreate();
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local').appName('Bf').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
from bitarray import bitarray
import mmh3
import math

def java_round(n):
    if n - math.floor(n) < 0.5:
        return math.floor(n)
    return math.ceil(n)
class BloomFilter:
    def __init__(self, m, fpr):
        self.fp_prob = fpr
        self.size = self.get_size(m, fpr)
        self.hash_count = self.get_hash_count(self.size, m)
        self.bit_array = bitarray(self.size)
        self.bit_array.setall(0)

    def add(self, t):
        for i in range(self.hash_count):
            digest = mmh3.hash(t, i) % self.size
            self.bit_array[digest] = True

    def getBitArray(self):
        return self.bit_array

    def check(self, t):
        for i in range(self.hash_count):
            digest = mmh3.hash(t, i) % self.size
            if self.bit_array[digest] == False:
                return False
        return True

    @classmethod
    def get_size(self, n, p):
        m = -(n * math.log(p))/(math.log(2)**2)
        return java_round(m)

    @classmethod
    def get_hash_count(self, m, n):
        k = (m/n) * math.log(2)
        return java_round(k)

file = sc.textFile("data.tsv")
temp = file.map(lambda x: (x.split("\t")))
titles = temp.map(lambda x: (x[0],java_round(float(x[1]))))
fpr = float(sys.argv[1])
bloom_filter = [0 for _ in range(10)]
for rating in range(10):
    titles_collected = titles.filter(lambda x : x[1] == rating + 1).collect()
    bloom_filter[rating] = BloomFilter(len(titles_collected), fpr)
    for title in range(0, len(titles_collected)):
        bloom_filter[rating].add(titles_collected[title][0])
    print("Rating: ", rating + 1, ", Added titles: ", len(titles_collected))
for rating in range(10):
    titles_collected = titles.filter(lambda x : x[1] != rating + 1).collect()
    count = 0
    for title in range(0, len(titles_collected)):
        if bloom_filter[rating].check(titles_collected[title][0]):
            count = count + 1
    print("Rating: ", rating + 1, ", Fp titles: ", count, ", Fpr: ", count / len(titles_collected))


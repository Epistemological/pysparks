from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    '''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
    '''
    conf = SparkConf().setAppName("primes").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    primes = sc.textFile("in/prime_nums.text")

    primes = primes.flatMap(lambda n: n.split("\t"))

    numbers = primes.filter(lambda n: n)

    intNumbers = numbers.map(lambda n: int(n))

    print("Sum is: {}".format(intNumbers.reduce(lambda x, y: x + y)))




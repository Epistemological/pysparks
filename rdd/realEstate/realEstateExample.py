import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}".format(splits[1])

def splitComma_2(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}".format(splits[3])

if __name__ == "__main__":
    conf = SparkConf().setAppName("realEstate").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    data = sc.textFile("in/RealEstate.csv")
    locations = data.map(lambda x: x.split(",")[1])

    # Distribution of locations
    #print("count: {}".format(locations.count()))

    #count_by_value = locations.countByValue()
    #print("count by value: ")
    #for title, amount in count_by_value.items():
    #   print("{}: {}".format(title, amount))

    # Bedrooms in Santa Maria
    location_santa_maria = data.filter(lambda x: x.split(",")[1] == "Santa Maria-Orcutt")
    bedrooms_santa_maria = location_santa_maria.map(splitComma_2)
    bedrooms_santa_maria_countByValue = bedrooms_santa_maria.countByValue()

    for beds, amount in bedrooms_santa_maria_countByValue.items():
        df = [(beds, amount)]
    bedrooms_santa_maria_sorted = sc.parallelize(df).sortBy(lambda x: x[1])

    print("bedroom alternatives in Santa Maria:")
    print("{}".format(bedrooms_santa_maria_sorted))

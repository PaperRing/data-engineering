from py4j.java_gateway import launch_gateway
from pyspark import SparkConf, SparkContext
import pandas as pd


conf = SparkConf().setMaster("local").setAppName("uber-trip-count")
sc = SparkContext(conf=conf)

directory = "/Users/junghee/data-engineering/01-spark/data"
filename = "fhvhv_tripdata_2020-03.csv"

lines = sc.textFile(f"file:///{directory}/{filename}")
header = lines.first()
filtered_lines = lines.filter(lambda row:row != header)

dates = filtered_lines.map(lambda x: x.split(",")[2].split(" ")[0])
result = dates.countByValue()

pd.Series(result, name="count_trip").to_csv("count_trips.csv")
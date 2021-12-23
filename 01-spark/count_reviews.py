# category 당 선호도
from pyspark import SparkConf, SparkContext
import pandas as pd

# Spark 설정
conf = SparkConf().setMaster("local").setAppName("restaurant-reviews")
sc = SparkContext(conf=conf)

# 데이터가 있는 파일
directory = "/Users/junghee/data-engineering/01-spark/data"
filename = "restaurant_reviews.csv"

# 데이터 파싱
lines = sc.textFile(f"file:///{directory}/{filename}")
header = lines.first() 
filtered_lines = lines.filter(lambda row:row != header) 

# 필요한 부분만 골라내서 파싱하는 function
def parse(filtered_lines):
  fields = filtered_lines.split(",")
  category = str(fields[2])
  reviews = int(fields[3])
  return (category, reviews)

# 카테고리별 평균 리뷰점수를 구하는 과정
categoryReviews = filtered_lines.map(parse)
countCategoryReviewss = categoryReviews.mapValues(lambda x: (x, 1))
reduced = countCategoryReviewss.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
result = reduced.mapValues(lambda x: (x[0] / x[1]))

print(result.collect())
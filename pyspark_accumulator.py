import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("covid-19").\
config("spark.driver.bindAddress","localhost").\
config("spark.ui.port","4050"). \
    getOrCreate()

csv_file = spark.read.csv("/Users/stephen/Desktop/data science/data/dataset-master/data.csv",header=True)


###定义两个dataframe

df_groupBy_data = csv_file.groupBy("State/UnionTerritory").count()
filter_data = csv_file.filter(col("State/UnionTerritory")=='Kerala')


###定义两个累加器

filter_accum=spark.sparkContext.accumulator(0)

df_groupBy_accum=spark.sparkContext.accumulator(0)



###使用累加器查看filter出来的行数

filter_data.foreach(lambda x: filter_accum.add(1))
print(filter_accum.value)


##使用累加器查看sum出来的总结果是多少

df_groupBy_data.foreach(lambda x: df_groupBy_accum.add(x[1]))

print("df_groupBy_accum.value=",df_groupBy_accum.value)


###cross validation查看用累加器算出来的结果和直接sum出来的结果是否一致

sum_amt = csv_file.groupBy("State/UnionTerritory").count()
sum_amt.groupBy().sum().show()
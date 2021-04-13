from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType)
import os
spark = SparkSession.builder.appName("Basics").getOrCreate()

rawpath = os.path.join(os.getcwd(),"./ignoreland/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/people.json")
df = spark.read.json(rawpath)

df.show()
# for json data:
df.printSchema()

df.columns # just like pandas
type(df.columns)  # list
df.describe().show()

# Structured field: name, type, nullable?
data_schema = [StructField("age", IntegerType(), True),
               StructField("name", StringType(), True)]
# declare the types
final_struc = StructType(fields=data_schema)
# Then read in the data
df = spark.read.json(rawpath, schema=final_struc)

#### Lec 26
df.select("age")  # returns a dataframe
df["age"] # returns a column

df.createOrReplaceTempView()
#### Lec 27
spark = SparkSession.builder.appName("ops").getOrCreate()
rawpath = os.path.join(os.getcwd(),"./ignoreland/Python-and-Spark-for-Big-Data-master/Spark_DataFrames")
os.listdir(rawpath)  # confirming that the file is where I think it is.
df = spark.read.csv(os.path.join(rawpath,"appl_stock.csv"), inferSchema=True, header=True)
df.printSchema()
"""
root
 |-- Date: string (nullable = true)
 |-- Open: double (nullable = true)
 |-- High: double (nullable = true)
 |-- Low: double (nullable = true)
 |-- Close: double (nullable = true)
 |-- Volume: integer (nullable = true)
 |-- Adj Close: double (nullable = true)
"""
type(df.head(3)[0])
# Filtering on multiple conditions
df.filter(df["Close"] < 200 & df["Open"] > 200).show()  # error results because conditions must be
# individually parenthesized.
df.filter((df["Close"] < 200) & (df["Open"] > 200)).show()  # this works.
df.filter((df["Close"] < 200) & ~ (df["Open"] > 200)).show()  # can negate second condition with tilde

# to save the result, we must .collect()
df_sunk00 = df.filter((df["Close"] < 200) & (df["Open"] > 200)).collect()
row_example = df_sunk00[0]  # all of the first row
row_example.asDict()  # treat the row as any other dict.
type(row_example.asDict())  # dict

#### Lec28: Groupby and aggregate functions
spark = SparkSession.builder.appName("agg").getOrCreate()
os.listdir(rawpath)  # confirming that the file is where I think it is.
df = spark.read.csv(os.path.join(rawpath, "sales_info.csv"), inferSchema=True, header=True)
df.show()
# try a groupby
df.groupBy("Company").mean().show()  # other arbitrary aggs available.  are there udfs?
df.agg({"Sales":"sum"}).show()

grouped_data = df.groupBy("Company")
grouped_data.agg({"Sales":"sum"}).show()  # we didn't need to hit .collect()
### import some spark sql functions
from pyspark.sql.functions import *   # not advised, but I want all the fcns
df.select(avg("Sales").alias("meansales")).show()  # put in the parens to create new col
sales_std = df.select(stddev("Sales").alias("std"))
sales_std.show()  # cool, need to trunc the decimals
sales_std.select(format_number("std", 2).alias("std_fmt")).show()
# this showed solving the problem in a few steps.

aa = df.orderBy("Sales", ascending=False).collect()
bb = df.orderBy(df["Sales"].desc()).collect()
type(aa)
type(bb)
aa == bb   # only true once we use the .collect() method

#### dropping rows with null values based on a threshold
df.na.drop(how="any")   # we can pass either thresh=n, or how, or subset (as in pandas)
df.na.fill()  # we can pass a string value, and spark will only plug this into the columns of relevant type
df.na.fill("Missing", subset=["Name"])  # better to be explicit
# grab the mean val object
mean_vals = df.select(mean(df["Sales"])).collect()  # returns a list
type(mean_vals), type(mean_vals[0])  # the first dimension slice is a pyspark Row
mean_vales_sales = mean_vals[0][0]  # subset to just get the scalar value
df.na.fill(mean_vales_sales, ["Sales"]).show()

#### dateparts
df = spark.read.csv(os.path.join(rawpath,"appl_stock.csv"), inferSchema=True, header=True)
df.select(dayofweek(df["Date"])).show()
# create a new column of the year
newdf = df.withColumn("Year", year(df["Date"]))  # no collect used
result = newdf.groupBy("Year").mean().select(["Year","avg(Close)"])
# rename messy column names
new = result.withColumnRenamed("avg(Close)", "close_mean")
new.select(["Year", format_number("close_mean",2).alias("close_mean")]).show()


#### we can add a column as so:
newdf = df.withColumn("Newcolumname", df["someval"]/df["someotherval"])
#### find id for highest metric value
df.orderBy(df["metric"].desc()).head(1)[0][0]  # assuming the id is the zeroth column.
#### filter meeting criteria
result = df.filter(df["metric"] < 60).count()  # count rows meeting criterion.
result.select(count("column"))  # or do it this way
#### fraction of rows meeting criterion
100 * df.filter(df["High"] > 200).count()*1.0 / df.count()   # use the *1.0 to do floating mult
#### get correlation btwn 2 cols
df.select(corr("High","Volume")).show()
#### Find max high price per year
yeardf = df.withColumn("Year", year(df["Date"])).groupBy("Year")
yeardf  # this is a GroupedData object
yeardf.max().select(["Year","max(High)"]).orderBy("Year").show()
#### Doing it the auto-column-name way to get avg close by month
monthdf = df.withColumn("Month", month(df["Date"]))
monthdf.show()
monthavg = monthdf.select(["Month","Close"]).groupBy("Month").mean()
monthavg.select(["Month","avg(Close)"]).orderBy("Month",ascending=False).show()
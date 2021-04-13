from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
import os

def cleancols(lst):
    """trim, space to underscores, lower"""
    outlist = []
    for colname in lst:
        colname = colname.strip().replace(" ", "_").lower()
        outlist.append(colname)
    return outlist

spark = SparkSession.builder.appName("lrex").getOrCreate()
rawpath = os.path.join(os.getcwd(),"./ignoreland/Python-and-Spark-for-Big-Data-master/Spark_for_Machine_Learning/Linear_Regression")
os.listdir(rawpath)  # confirming that the file is where I think it is.

## Lib svm is pre-packaged for sparkml # note the possible speed gain from pre-specifying numFeatures
training = spark.read.format("libsvm").load(os.path.join(rawpath, "sample_linear_regression_data.txt"))
training.show()

lr = LinearRegression(featuresCol="features", labelCol="label",
                      predictionCol="prediction")  # prediction is the name of the generated pred

lrModel = lr.fit(training)  # no regParam set
lrModel.coefficients
lrModel.intercept

training_summary = lrModel.summary
training_summary.__dict__  # actually a java obj.
training_summary.__dir__()
training_summary.rootMeanSquaredError  #, etc.

full_data = spark.read.format("libsvm").load(os.path.join(rawpath, "sample_linear_regression_data.txt"))
train_data, test_data = full_data.randomSplit(([0.7, 0.3]))

trainedModel = lr.fit(train_data)
test_performance = trainedModel.evaluate(test_data)
test_performance.rootMeanSquaredError

test_features = test_data.select("features")  # subset to the covariates/remove Ys
# it's called "transform" when we deploy to unseen data to get predictions.
predictions = trainedModel.transform(test_features)
type(predictions)  # this is a pyspark DataFrame; ie, test_features + prediction

###########################################################################
# lec38
# ecommerce data spend prediction.
full_data = spark.read.csv(os.path.join(rawpath, "Ecommerce_Customers.csv"), header=True, inferSchema=True)
full_data.printSchema()
type(full_data)

cleanedcols = cleancols(full_data.columns)

full_data = full_data.toDF(*cleanedcols)   # this may not be reasonable
type(full_data)
"""
Necessary set up for pyspark ml
Create an assembler to pack the features
call assembler.transform() on the full data
"""
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=cleanedcols[:-1],
                            outputCol="features")  # packs into the summary feature

output = assembler.transform(full_data)


# we are predicting yearly_amount_spend
train_data, test_data = full_data.randomSplit(([0.7, 0.3]))


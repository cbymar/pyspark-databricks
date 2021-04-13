from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
import os

from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   MulticlassClassificationEvaluator)


spark = SparkSession.builder.appName("logit").getOrCreate()
rawpath = os.path.join(os.getcwd(),"./ignoreland/Python-and-Spark-for-Big-Data-master/Spark_for_Machine_Learning/Logistic_Regression")
os.listdir(rawpath)  # confirming that the file is where I think it is.

df_all = spark.read.format("libsvm").load(os.path.join(rawpath, "sample_libsvm_data.txt"))

train_data, test_data = df_all.randomSplit(([0.65, 0.35]), seed=869)

lr_model = LogisticRegression()
fitted_lr = lr_model.fit(train_data)

prediction_and_labels = fitted_lr.evaluate(test_data)
prediction_and_labels.predictions.show()
#### Apply evaluators.

my_eval = BinaryClassificationEvaluator()
final_roc = my_eval.evaluate(prediction_and_labels.predictions)
final_roc

#### Code along for titanic
os.listdir(rawpath)  # confirming that the file is where I think it is.
df_all = spark.read.csv(os.path.join(rawpath, "titanic.csv"), header=True, inferSchema=True)
df_all.printSchema()
df_all = df_all.dropna(how="any")

from pyspark.ml.feature import (VectorAssembler, VectorIndexer, OneHotEncoder,StringIndexer)
## Firststep for onehot (orthogonal) encoding
gender_indexer = StringIndexer(inputCol="Sex", outputCol="SexIndex")
gender_encoder = OneHotEncoder(inputCol="SexIndex", outputCol="SexVec")

embark_indexer = StringIndexer(inputCol="Embarked", outputCol="EmbarkIndex")
embark_encoder = OneHotEncoder(inputCol="EmbarkIndex", outputCol="EmbarkVec")
df_all.columns
assembler = VectorAssembler(inputCols=["Pclass", "SexVec", "EmbarkVec", "Age", "SibSp"],
                            outputCol="features")

## Declare a pipeline
from pyspark.ml import Pipeline
log_reg_titanic = LogisticRegression(featuresCol="features", labelCol="Survived")
pipeline = Pipeline(stages=[gender_indexer, embark_indexer,
                            gender_encoder, embark_encoder,
                            assembler, log_reg_titanic])

train_data, test_data = df_all.randomSplit(([0.65, 0.35]), seed=869)
fit_model = pipeline.fit(train_data)
results = fit_model.transform(test_data)
results.select("Survived", "prediction")

my_eval = BinaryClassificationEvaluator(rawPredictionCol="prediction", labelCol="Survived")
my_eval.evaluate(results)
my_eval.getMetricName()
#### https://spark.apache.org/docs/3.1.1/ml-classification-regression.html#binomial-logistic-regression

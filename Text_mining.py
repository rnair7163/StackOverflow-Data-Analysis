import pyspark
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import SQLContext
from bs4 import BeautifulSoup
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, OneVsRest, RandomForestClassifier
from pyspark.ml.feature import IDF, StringIndexer, StopWordsRemover, CountVectorizer, RegexTokenizer, IndexToString
import nltk
from nltk.corpus import stopwords

# creating schema for the dataframe
qSchema = StructType([StructField('post', StringType(), True),
                     StructField('tags', StringType(), True)])
d = pd.read_csv("/Users/rahulnair/desktop/stack-overflow-data.csv")

sqlContext = SQLContext(sc)
sdf = sqlContext.createDataFrame(d, qSchema)

# filtering out null values
sdf = sdf.filter(sdf.tags.isNotNull())
print("the dataframe for the data: ", sdf)

# Splitting the data 
(train, test) = sdf.randomSplit((0.75, 0.25), seed = 100)

# For removing HTML tags
class BsTextExtractor(Transformer, HasInputCol, HasOutputCol):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(BsTextExtractor, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):

        def f(s):
            cleaned_post = BeautifulSoup(s).text
            return cleaned_post

        t = StringType()
        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]
        return dataset.withColumn(out_col, udf(f, t)(in_col))
    
nltk.download('stopwords')

# list of stopwords to be removed from the posts
StopWords = list(set(stopwords.words('english')))

labelIndexer = StringIndexer(inputCol="tags", outputCol="label").fit(train)
bs_text_extractor = BsTextExtractor(inputCol="post", outputCol="untagged_post")
RegexTokenizer = RegexTokenizer(inputCol=bs_text_extractor.getOutputCol(), outputCol="words", pattern="[^0-9a-z#+_]+")
StopwordRemover = StopWordsRemover(inputCol=RegexTokenizer.getOutputCol(), outputCol="filtered_words").setStopWords(
    StopWords)
CountVectorizer = CountVectorizer(inputCol=StopwordRemover.getOutputCol(), outputCol="countFeatures", minDF=5)
idf = IDF(inputCol=CountVectorizer.getOutputCol(), outputCol="features")
rf = RandomForestClassifier(labelCol="label", featuresCol=idf.getOutputCol(), numTrees=100, maxDepth=4)
idx_2_string = IndexToString(inputCol="prediction", outputCol="predictedValue")
idx_2_string.setLabels(labelIndexer.labels)

# creating the pipeline
pipeline = Pipeline(stages=[
    labelIndexer,
    bs_text_extractor,
    RegexTokenizer,
    StopwordRemover,
    CountVectorizer,
    idf,
    rf,
    idx_2_string])

# fitting the model
model = pipeline.fit(train)

# performing the prediction
predictions = model.transform(test)

# convert spark dataframe to Pandas Dataframe
qwerty = predictions.toPandas()
print("the Predictions are: ", qwerty)

# evaluating the model
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
evaluator.evaluate(predictions)

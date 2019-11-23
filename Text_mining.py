import pyspark
from pyspark.sql.types import *
from bs4 import BeautifulSoup
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, OneVsRest, RandomForestClassifier
from pyspark.ml.feature import IDF, StringIndexer, StopWordsRemover, CountVectorizer, RegexTokenizer
import nltk
from nltk.corpus import stopwords   
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder



qSchema = StructType([StructField('post', StringType(), True),
                     StructField('tags', StringType(), True)])
                     
d = pd.read_csv("/Users/rahulnair/desktop/stack-overflow-data.csv")
d.head()

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

sdf = sqlContext.createDataFrame(d, qSchema)

sdf.show(3)

sdf.groupby('tags').count().show(50)

sdf = sdf.filter(sdf.tags.isNotNull())
sdf.groupby('tags').count().show(50)

(train_df, test_df) = sdf.randomSplit((0.75, 0.25), seed = 100)

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
  
bs_text_extractor = BsTextExtractor(inputCol="post", outputCol="cleaned_post")
text_extracted = bs_text_extractor.transform(sdf)

nltk.download('stopwords')
stop_words = list(set(stopwords.words('english')))

label_stringIdx = StringIndexer(inputCol="tags", outputCol="label")
bs_text_extractor = BsTextExtractor(inputCol="post", outputCol="untagged_post")
regex_tokenizer = RegexTokenizer(inputCol=bs_text_extractor.getOutputCol(), outputCol="words", pattern="[^0-9a-z#+_]+")
stopword_remover = StopWordsRemover(inputCol=regex_tokenizer.getOutputCol(), outputCol="filtered_words").setStopWords(
    stop_words)
count_vectorizer = CountVectorizer(inputCol=stopword_remover.getOutputCol(), outputCol="countFeatures", minDF=5)
idf = IDF(inputCol=count_vectorizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(featuresCol=idf.getOutputCol(), labelCol="label")
rf = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol="label", seed=42, featuresCol=idf.getOutputCol())

pipeline = Pipeline(stages=[
    label_stringIdx,
    bs_text_extractor,
    regex_tokenizer,
    stopword_remover,
    count_vectorizer,
    idf,
    rf])
    
   model = pipeline.fit(train_df)
   predictions = model.transform(test_df)
   
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

evaluator.evaluate(predictions)



paramGrid = (ParamGridBuilder()
  .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
  .addGrid(lr.maxIter, [10, 20, 50]) \
  .addGrid(lr.elasticNetParam, [0.0, 0.8]) \
  .build())

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=3)
                          
model = crossval.fit(train_df)

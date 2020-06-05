```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SpamDetection").getOrCreate()
```


```python
from pyspark.sql.types import StructField,StructType,StringType

Schema=StructType([
    StructField('label',StringType()),
    StructField('text',StringType())
])
```


```python
sms=spark.read.csv("D:\Spark_edureka\Module_7\spam_dataset\SMSSpamCollection.csv",header=True,schema=Schema)
```


```python
sms.printSchema()
```

    root
     |-- label: string (nullable = true)
     |-- text: string (nullable = true)
    
    


```python
sms.show(2,False)
```

    +-----+---------------------------------------------------------------------------------------------------------------+
    |label|text                                                                                                           |
    +-----+---------------------------------------------------------------------------------------------------------------+
    |ham  |Go until jurong point, crazy.. Available only in bugis n great world la e buffet... Cine there got amore wat...|
    |ham  |Ok lar... Joking wif u oni...                                                                                  |
    +-----+---------------------------------------------------------------------------------------------------------------+
    only showing top 2 rows
    
    


```python
from pyspark.sql.functions import regexp_replace
wrangled=sms.withColumn('text',regexp_replace('text','[_():;,.!?\\-,\d]',' ')).withColumn('text',regexp_replace('text',' +',' '))
```


```python
wrangled.show(2,False)
```

    +-----+-------------------------------------------------------------------------------------------------------+
    |label|text                                                                                                   |
    +-----+-------------------------------------------------------------------------------------------------------+
    |ham  |Go until jurong point crazy Available only in bugis n great world la e buffet Cine there got amore wat |
    |ham  |Ok lar Joking wif u oni                                                                                |
    +-----+-------------------------------------------------------------------------------------------------------+
    only showing top 2 rows
    
    


```python
from pyspark.sql.functions import when,col
wrangled=wrangled.withColumn('label',when(col('label')=='spam',0).otherwise(1))
wrangled.show(2)
```

    +-----+--------------------+
    |label|                text|
    +-----+--------------------+
    |    1|Go until jurong p...|
    |    1|Ok lar Joking wif...|
    +-----+--------------------+
    only showing top 2 rows
    
    


```python
from pyspark.ml.feature import Tokenizer, StopWordsRemover,CountVectorizer

StopwordList=['-']
StopwordList.extend(StopWordsRemover().getStopWords())

tokenizer=Tokenizer(inputCol='text',outputCol='words')
remover=StopWordsRemover(inputCol='words',outputCol='terms',stopWords=StopwordList)
cv = CountVectorizer(inputCol='terms', outputCol="features",vocabSize=1000)
```


```python
from pyspark.ml import Pipeline
pipeline=Pipeline(stages=[tokenizer,remover,cv])
```


```python

pipeline_fit=pipeline.fit(wrangled)
wrangled=pipeline_fit.transform(wrangled)
wrangled.show(5)
```

    +-----+--------------------+--------------------+--------------------+--------------------+
    |label|                text|               words|               terms|            features|
    +-----+--------------------+--------------------+--------------------+--------------------+
    |    1|Go until jurong p...|[go, until, juron...|[go, jurong, poin...|(1000,[7,13,25,53...|
    |    1|Ok lar Joking wif...|[ok, lar, joking,...|[ok, lar, joking,...|(1000,[0,6,239,36...|
    |    0|Free entry in a w...|[free, entry, in,...|[free, entry, wkl...|(1000,[8,20,22,91...|
    |    1|U dun say so earl...|[u, dun, say, so,...|[u, dun, say, ear...|(1000,[0,78,80,81...|
    |    1|Nah I don't think...|[nah, i, don't, t...|[nah, think, goes...|(1000,[48,132,368...|
    +-----+--------------------+--------------------+--------------------+--------------------+
    only showing top 5 rows
    
    


```python
training,test=wrangled.randomSplit([0.8,0.2],seed=123)

print('training: '+str(training.count()))
print('test: '+str(test.count()))
```

    training: 4461
    test: 1113
    


```python
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

logistic = LogisticRegression().fit(training)
predictions=logistic.transform(test)


evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions)



```




    0.9580034695734232



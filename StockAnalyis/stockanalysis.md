```python
from pyspark.sql import SparkSession
```


```python
spark=spark.builder.appName('StockAnalysis').getOrCreate()
```


```python
spark
```





    <div>
        <p><b>SparkSession - hive</b></p>

<div>
    <p><b>SparkContext</b></p>

    <p><a href="http://MSI:4040">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.0.0-preview2</code></dd>
      <dt>Master</dt>
        <dd><code>local[*]</code></dd>
      <dt>AppName</dt>
        <dd><code>PySparkShell</code></dd>
    </dl>
</div>

    </div>





```python
stockAAON=spark.read.csv("D:\\Big_data_learning\\mobileAppData\\5g77sh0a1o\\Spark_Sql Datasets\\614_m6_StockMarket_DataSets\\AAON.csv",header=True,inferSchema=True)
stockABAX=spark.read.csv("D:\\Big_data_learning\\mobileAppData\\5g77sh0a1o\\Spark_Sql Datasets\\614_m6_StockMarket_DataSets\\ABAX.csv",header=True,inferSchema=True)
stockFAST=spark.read.csv("D:\\Big_data_learning\\mobileAppData\\5g77sh0a1o\\Spark_Sql Datasets\\614_m6_StockMarket_DataSets\\FAST.csv",header=True,inferSchema=True)
stockFFIV=spark.read.csv("D:\\Big_data_learning\\mobileAppData\\5g77sh0a1o\\Spark_Sql Datasets\\614_m6_StockMarket_DataSets\\FFIV.csv",header=True,inferSchema=True)
stockGILD=spark.read.csv("D:\\Big_data_learning\\mobileAppData\\5g77sh0a1o\\Spark_Sql Datasets\\614_m6_StockMarket_DataSets\\GILD.csv",header=True,inferSchema=True)
stockMSFT=spark.read.csv("D:\\Big_data_learning\\mobileAppData\\5g77sh0a1o\\Spark_Sql Datasets\\614_m6_StockMarket_DataSets\\MSFT.csv",header=True,inferSchema=True)
stockORLY=spark.read.csv("D:\\Big_data_learning\\mobileAppData\\5g77sh0a1o\\Spark_Sql Datasets\\614_m6_StockMarket_DataSets\\ORLY.csv",header=True,inferSchema=True)
stockPCAR=spark.read.csv("D:\\Big_data_learning\\mobileAppData\\5g77sh0a1o\\Spark_Sql Datasets\\614_m6_StockMarket_DataSets\\PCAR.csv",header=True,inferSchema=True)
stockSHLM=spark.read.csv("D:\\Big_data_learning\\mobileAppData\\5g77sh0a1o\\Spark_Sql Datasets\\614_m6_StockMarket_DataSets\\SHLM.csv",header=True,inferSchema=True)
stockWYNN=spark.read.csv("D:\\Big_data_learning\\mobileAppData\\5g77sh0a1o\\Spark_Sql Datasets\\614_m6_StockMarket_DataSets\\WYNN.csv",header=True,inferSchema=True)
```


```python
stockAAON.show(5)

```

    +----------+---------+---------+---------+---------+------+---------+
    |      Date|     Open|     High|      Low|    Close|Volume|Adj Close|
    +----------+---------+---------+---------+---------+------+---------+
    |2015-12-31|    23.57|    23.84|23.190001|23.219999|211100|23.036494|
    |2015-12-30|     23.5|    23.84|     23.5|    23.67|131500|23.482938|
    |2015-12-29|23.620001|23.709999|23.290001|    23.52|220900|23.334124|
    |2015-12-28|23.639999|    23.77|23.379999|    23.52|138700|23.334124|
    |2015-12-24|23.389999|23.799999|     23.0|    23.68|102000|23.492859|
    +----------+---------+---------+---------+---------+------+---------+
    only showing top 5 rows
    
    


```python
stockAAON.dtypes
```




    [('Date', 'string'),
     ('Open', 'double'),
     ('High', 'double'),
     ('Low', 'double'),
     ('Close', 'double'),
     ('Volume', 'int'),
     ('Adj Close', 'double')]




```python
stockAAON.printSchema()
```

    root
     |-- Date: string (nullable = true)
     |-- Open: double (nullable = true)
     |-- High: double (nullable = true)
     |-- Low: double (nullable = true)
     |-- Close: double (nullable = true)
     |-- Volume: integer (nullable = true)
     |-- Adj Close: double (nullable = true)
    
    


```python
from pyspark.sql import functions as F
stockAAON.select(F.year('Date').alias('year'),F.month('Date').alias('month'),'Adj Close').groupBy('year','month')\
        .agg(F.avg('Adj Close')).sort('year','month',ascending=False).show()
```

    +----+-----+------------------+
    |year|month|    avg(Adj Close)|
    +----+-----+------------------+
    |2015|   12|23.335025227272727|
    |2015|   11|23.803906299999998|
    |2015|   10|20.489242090909094|
    |2015|    9|19.646504952380944|
    |2015|    8| 21.68787128571429|
    |2015|    7|22.307166045454547|
    |2015|    6|23.154616454545454|
    |2015|    5|       23.29530975|
    |2015|    4|23.744573142857146|
    |2015|    3| 22.44942336363636|
    |2015|    2|         22.883619|
    |2015|    1|       21.46316645|
    |2014|   12|21.128395590909093|
    |2014|   11| 20.32426563157895|
    |2014|   10|17.630752739130436|
    |2014|    9|17.888005761904758|
    |2014|    8| 18.55884695238095|
    |2014|    7|20.849365090909096|
    |2014|    6|21.223501857142853|
    |2014|    5|19.866252999999997|
    +----+-----+------------------+
    only showing top 20 rows
    
    


```python
stockMSFT.show(2)
```

    +----------+---------+---------+---------+---------+--------+---------+
    |      Date|     Open|     High|      Low|    Close|  Volume|Adj Close|
    +----------+---------+---------+---------+---------+--------+---------+
    |2015-12-31|56.040001|56.189999|55.419998|    55.48|26529600|53.998276|
    |2015-12-30|56.470001|56.779999|56.290001|56.310001|21704500| 54.80611|
    +----------+---------+---------+---------+---------+--------+---------+
    only showing top 2 rows
    
    


```python
from pyspark.sql.functions import col,abs
stockMSFT.withColumn('MSFTDIFF',abs(col('close')-col('open')))\
.filter(col('MSFTDIFF')>2)\
.select('Date','open','close','MSFTDIFF').show()
```

    +----------+---------+---------+------------------+
    |      Date|     open|    close|          MSFTDIFF|
    +----------+---------+---------+------------------+
    |2015-08-25|    42.57|40.470001| 2.099998999999997|
    |2015-08-21|45.299999|    43.07|2.2299989999999994|
    |2015-04-24|    45.66|47.869999|2.2099990000000034|
    |2008-10-13|22.860001|     25.5|2.6399989999999995|
    +----------+---------+---------+------------------+
    
    


```python
joinclose=stockAAON.join(stockABAX,on='Date',how='inner')\
.join(stockFAST,on='Date',how='inner')\
.select('Date',stockAAON['Close'].alias('AAONClose'),stockABAX['Close'].alias('ABAXClose'),stockFAST['Close'].alias('FASTClose'))
```


```python
joinclose.show()
```

    +----------+---------+---------+---------+
    |      Date|AAONClose|ABAXClose|FASTClose|
    +----------+---------+---------+---------+
    |2015-12-31|23.219999|    55.68|    40.82|
    |2015-12-30|    23.67|57.880001|41.509998|
    |2015-12-29|    23.52|     57.0|41.639999|
    |2015-12-28|    23.52|    55.98|41.400002|
    |2015-12-24|    23.68|54.970001|41.509998|
    |2015-12-23|23.209999|54.779999|41.259998|
    |2015-12-22|     23.1|53.990002|40.080002|
    |2015-12-21|    22.85|52.549999|    38.84|
    |2015-12-18|    22.76|51.169998|    39.09|
    |2015-12-17|    23.08|    52.91|39.990002|
    |2015-12-16|23.389999|53.540001|40.290001|
    |2015-12-15|23.379999|52.200001|39.830002|
    |2015-12-14|    23.51|53.009998|39.900002|
    |2015-12-11|    22.74|51.700001|39.470001|
    |2015-12-10|    23.33|    53.59|40.150002|
    |2015-12-09|    23.42|53.049999|40.220001|
    |2015-12-08|    24.34|54.220001|40.200001|
    |2015-12-07|24.059999|54.200001|40.830002|
    |2015-12-04|24.530001|54.290001|40.139999|
    |2015-12-03|    23.74|53.900002|39.439999|
    +----------+---------+---------+---------+
    only showing top 20 rows
    
    


```python
joinclose.write.parquet('D://Big_data_learning//Stock.parquet')
```


```python
df=spark.read.parquet('D:\\Big_data_learning\\Stock.parquet')
```


```python
df.show()
```

    +----------+---------+---------+---------+
    |      Date|AAONClose|ABAXClose|FASTClose|
    +----------+---------+---------+---------+
    |2015-12-31|23.219999|    55.68|    40.82|
    |2015-12-30|    23.67|57.880001|41.509998|
    |2015-12-29|    23.52|     57.0|41.639999|
    |2015-12-28|    23.52|    55.98|41.400002|
    |2015-12-24|    23.68|54.970001|41.509998|
    |2015-12-23|23.209999|54.779999|41.259998|
    |2015-12-22|     23.1|53.990002|40.080002|
    |2015-12-21|    22.85|52.549999|    38.84|
    |2015-12-18|    22.76|51.169998|    39.09|
    |2015-12-17|    23.08|    52.91|39.990002|
    |2015-12-16|23.389999|53.540001|40.290001|
    |2015-12-15|23.379999|52.200001|39.830002|
    |2015-12-14|    23.51|53.009998|39.900002|
    |2015-12-11|    22.74|51.700001|39.470001|
    |2015-12-10|    23.33|    53.59|40.150002|
    |2015-12-09|    23.42|53.049999|40.220001|
    |2015-12-08|    24.34|54.220001|40.200001|
    |2015-12-07|24.059999|54.200001|40.830002|
    |2015-12-04|24.530001|54.290001|40.139999|
    |2015-12-03|    23.74|53.900002|39.439999|
    +----------+---------+---------+---------+
    only showing top 20 rows
    
    


```python
from pyspark.sql.functions import year
df1=df.withColumn('year',year('Date'))
df1.show()
```

    +----------+---------+---------+---------+----+
    |      Date|AAONClose|ABAXClose|FASTClose|year|
    +----------+---------+---------+---------+----+
    |2015-12-31|23.219999|    55.68|    40.82|2015|
    |2015-12-30|    23.67|57.880001|41.509998|2015|
    |2015-12-29|    23.52|     57.0|41.639999|2015|
    |2015-12-28|    23.52|    55.98|41.400002|2015|
    |2015-12-24|    23.68|54.970001|41.509998|2015|
    |2015-12-23|23.209999|54.779999|41.259998|2015|
    |2015-12-22|     23.1|53.990002|40.080002|2015|
    |2015-12-21|    22.85|52.549999|    38.84|2015|
    |2015-12-18|    22.76|51.169998|    39.09|2015|
    |2015-12-17|    23.08|    52.91|39.990002|2015|
    |2015-12-16|23.389999|53.540001|40.290001|2015|
    |2015-12-15|23.379999|52.200001|39.830002|2015|
    |2015-12-14|    23.51|53.009998|39.900002|2015|
    |2015-12-11|    22.74|51.700001|39.470001|2015|
    |2015-12-10|    23.33|    53.59|40.150002|2015|
    |2015-12-09|    23.42|53.049999|40.220001|2015|
    |2015-12-08|    24.34|54.220001|40.200001|2015|
    |2015-12-07|24.059999|54.200001|40.830002|2015|
    |2015-12-04|24.530001|54.290001|40.139999|2015|
    |2015-12-03|    23.74|53.900002|39.439999|2015|
    +----------+---------+---------+---------+----+
    only showing top 20 rows
    
    


```python
from pyspark.sql.functions import round,avg,col
df2=df1.groupBy('Year').agg(round(avg('AAONClose'),3).alias('AAON'),
                      round(avg('ABAXClose'),3).alias('ABAX'),
                      round(avg('FASTClose'),3).alias('FAST')).sort('year',ascending=False)
df2.show()
```

    +----+------+------+------+
    |Year|  AAON|  ABAX|  FAST|
    +----+------+------+------+
    |2015|22.659|54.136|41.049|
    |2014|25.274|45.571|46.678|
    |2013|26.908|42.003|  48.6|
    |2012|19.624|33.652|45.097|
    |2011|24.653|26.846|46.153|
    |2010|23.519|23.868|50.527|
    |2009|19.297|20.709|35.409|
    |2008|18.642|22.887| 43.75|
    |2007|25.239|24.038|41.297|
    |2006| 23.45|21.096|40.206|
    |2005|17.065|12.419|57.886|
    +----+------+------+------+
    
    


```python
df2.createTempView('df3')
```


```python
companyall=sqlContext.sql('select year,AAON as Value,"AAON" as Company from df3 union all \
                          select year,ABAX as Value,"ABAX" as Company from df3 union all \
                         select year,FAST as Value,"FAST" as Company from df3').cache()
```


```python
companyall.show()
```

    +----+------+-------+
    |year| Value|Company|
    +----+------+-------+
    |2015|22.659|   AAON|
    |2014|25.274|   AAON|
    |2013|26.908|   AAON|
    |2012|19.624|   AAON|
    |2011|24.653|   AAON|
    |2010|23.519|   AAON|
    |2009|19.297|   AAON|
    |2008|18.642|   AAON|
    |2007|25.239|   AAON|
    |2006| 23.45|   AAON|
    |2005|17.065|   AAON|
    |2015|54.136|   ABAX|
    |2014|45.571|   ABAX|
    |2013|42.003|   ABAX|
    |2012|33.652|   ABAX|
    |2011|26.846|   ABAX|
    |2010|23.868|   ABAX|
    |2009|20.709|   ABAX|
    |2008|22.887|   ABAX|
    |2007|24.038|   ABAX|
    +----+------+-------+
    only showing top 20 rows
    
    


```python
companyall.createTempView('companyall')
```


```python
BestCompanyYear=sqlContext.sql('select year,Greatest((AAON),(ABAX),(FAST)) as BestCompany from df3')
```


```python
BestCompanyYear.show()
```

    +----+-----------+
    |year|BestCompany|
    +----+-----------+
    |2015|     54.136|
    |2014|     46.678|
    |2013|       48.6|
    |2012|     45.097|
    |2011|     46.153|
    |2010|     50.527|
    |2009|     35.409|
    |2008|      43.75|
    |2007|     41.297|
    |2006|     40.206|
    |2005|     57.886|
    +----+-----------+
    
    


```python
BestCompanyYear.createTempView('BestCompanyYear')
```


```python
finaltable=sqlContext.sql('select companyall.year,BestCompanyYear.BestCompany,companyall.company from BestCompanyYear\
                           left join  companyall on companyall.value=BestCompanyYear.BestCompany order by companyall.year')
```


```python
finaltable.show()
```

    +----+-----------+-------+
    |year|BestCompany|company|
    +----+-----------+-------+
    |2005|     57.886|   FAST|
    |2006|     40.206|   FAST|
    |2007|     41.297|   FAST|
    |2008|      43.75|   FAST|
    |2009|     35.409|   FAST|
    |2010|     50.527|   FAST|
    |2011|     46.153|   FAST|
    |2012|     45.097|   FAST|
    |2013|       48.6|   FAST|
    |2014|     46.678|   FAST|
    |2015|     54.136|   ABAX|
    +----+-----------+-------+
    
    


```python
from pyspark.mllib.stat import Statistics
df1.stat.corr('AAONClose','ABAXClose', method="pearson")

```




    0.2935736821738406




```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

vector_col = "corr_features"
assembler = VectorAssembler(inputCols=['AAONClose','ABAXClose'], outputCol=vector_col)
df_vector = assembler.transform(df1).select(vector_col)

matrix = Correlation.corr(df_vector, vector_col,
                         'pearson')
matrix.show(truncate=False)
```

    +---------------------------------------------------------------------------------+
    |pearson(corr_features)                                                           |
    +---------------------------------------------------------------------------------+
    |1.0                 0.2935736821738398  
    0.2935736821738398  1.0                 |
    +---------------------------------------------------------------------------------+
    
    



BIG DATA ANALYTICS
Project Part 2 -Analysis of Amazon Review Dataset
Report by Vallabhan Kudlu Ramakrishnan
The University of Texas at Dallas
May 2020



















































































When purchasing a product only variable we look into are rating and reviews. Improving it is
important to maintain current customers and will draw new customers. This project aims to
identify relationships between different aspects of a review including review length, overall review
star rating, review helpfulness, reviewer frequency, product price, etc.

This project uses advanced tools to research the different statistical patterns related to these
aspects of the analysis. The Relation were analyzed using Spark,AWS EMR and Power BI





The workflow of this project involves four stages:
• Data collection and preparation • Analysis using Pyspark • Conclusions derived from analysis •
Visualizations
The Amazon Test Dataset is available online free of charge on the SNAP datasets website.
The list contains customer reviews of items that are offered on the Amazon website. There are
two pieces of this dataset. The first component includes about 35 million reviews spanning 13
years of data from product feedback. It includes user-related information that offers analysis,
time-related information, and analysis features such as review length, description, etc.

In order to understand the structure and schema of the dataset, let us look at a sample Amazon
Review.

![image-20200504230112942](C:\Users\vallabhan\AppData\Roaming\Typora\typora-user-images\image-20200504230112942.png)

• Summary : The title of the review
• Review text : The actual content of the review.
• Rating : User rating of the product on a scale of 1 to 5.
• Helpfulness : The number of people who found the review useful.





## Analysis

Run the Pyspark on EMR


```pyspark3
spark
```

    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>0</td><td>application_1588645875432_0004</td><td>pyspark3</td><td>idle</td><td><a target="_blank" href="http://ip-172-31-36-186.ec2.internal:20888/proxy/application_1588645875432_0004/">Link</a></td><td><a target="_blank" href="http://ip-172-31-43-111.ec2.internal:8042/node/containerlogs/container_1588645875432_0004_01_000001/livy">Link</a></td><td>✔</td></tr></table>


    SparkSession available as 'spark'.
    <pyspark.sql.session.SparkSession object at 0x7f20a18d1b38>


```pyspark3
from pyspark.sql import functions as F
```


```pyspark3
# Load Data Set
df = spark.read\
          .option("header", "true")\
          .option("inferSchema", "true")\
          .option("basePath", "hdfs:///hive/amazon-reviews-pds/parquet/")\
          .parquet("hdfs:///hive/amazon-reviews-pds/parquet/*")
```


```pyspark3
df.printSchema()
```

    root
     |-- marketplace: string (nullable = true)
     |-- customer_id: string (nullable = true)
     |-- review_id: string (nullable = true)
     |-- product_id: string (nullable = true)
     |-- product_parent: string (nullable = true)
     |-- product_title: string (nullable = true)
     |-- star_rating: integer (nullable = true)
     |-- helpful_votes: integer (nullable = true)
     |-- total_votes: integer (nullable = true)
     |-- vine: string (nullable = true)
     |-- verified_purchase: string (nullable = true)
     |-- review_headline: string (nullable = true)
     |-- review_body: string (nullable = true)
     |-- review_date: date (nullable = true)
     |-- year: integer (nullable = true)
     |-- product_category: string (nullable = true)


```pyspark3
df.columns
```

    ['marketplace', 'customer_id', 'review_id', 'product_id', 'product_parent', 'product_title', 'star_rating', 'helpful_votes', 'total_votes', 'vine', 'verified_purchase', 'review_headline', 'review_body', 'review_date', 'year', 'product_category']


```pyspark3
columns_to_keep = ['customer_id', 'review_id', 'product_id', 'product_parent', 
                   'product_title', 'star_rating', 'helpful_votes', 'total_votes',
                   'verified_purchase', 'review_date', 'year', 'product_category','review_body']
df_limited = df.select(columns_to_keep).filter(F.col("year")>2004)
```


```pyspark3
df_limited.printSchema()
```

    root
     |-- customer_id: string (nullable = true)
     |-- review_id: string (nullable = true)
     |-- product_id: string (nullable = true)
     |-- product_parent: string (nullable = true)
     |-- product_title: string (nullable = true)
     |-- star_rating: integer (nullable = true)
     |-- helpful_votes: integer (nullable = true)
     |-- total_votes: integer (nullable = true)
     |-- verified_purchase: string (nullable = true)
     |-- review_date: date (nullable = true)
     |-- year: integer (nullable = true)
     |-- product_category: string (nullable = true)
     |-- review_body: string (nullable = true)


```pyspark3
df_limited.rdd.getNumPartitions()

```

    216

## Cleaning the Data


```pyspark3
from pyspark.sql.functions import row_number
from pyspark.sql.window import *
df_new=df_limited.withColumn("row_num", row_number().over(Window.partitionBy("customer_id","product_id","product_category").orderBy("review_date"))).select("row_num","customer_id","review_id","product_id","product_parent","product_title","star_rating","helpful_votes","total_votes","verified_purchase","review_date","year","product_category","review_body")
```


```pyspark3
df_new1=df_new.where(F.col('row_num')==1).drop('row_num')
```


```pyspark3
df_new1.show(5)
```

    +-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+-----------------+-----------+----+----------------+--------------------+
    |customer_id|     review_id|product_id|product_parent|       product_title|star_rating|helpful_votes|total_votes|verified_purchase|review_date|year|product_category|         review_body|
    +-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+-----------------+-----------+----+----------------+--------------------+
    |   10000113| RQ9E9A2ISWOQV|B0083XTNJK|     416505290|CrazyOnDigital Re...|          4|            0|          0|                Y| 2012-10-02|2012|              PC|I am very pleased...|
    |   10000166|R3AAIE04RH1VKI|B00LDCJ190|      39152592|Leprechaun: Origi...|          3|            0|          0|                N| 2014-10-15|2014|       Video_DVD|while not an awes...|
    |   10000185|R1437UFF5891NA|0061986380|     364246210|Extinction Agenda...|          5|            0|          0|                N| 2011-12-29|2011|           Books|Great book. I fin...|
    |    1000021| RSI49CIWKMLHW|B00OB1IS08|     635789129|Note 4 Case, E LV...|          5|            0|          0|                Y| 2015-05-24|2015|        Wireless|So excited for th...|
    |   10000221| RLMBSUF00TJIV|B004ZMH9OE|     519764619|TRIPP LITE USB 3....|          5|            0|          1|                Y| 2013-01-21|2013|              PC|we looked at a nu...|
    +-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+-----------------+-----------+----+----------------+--------------------+
    only showing top 5 rows


```pyspark3
df_new1.count()
```

    65911069


```pyspark3
df_new1.persist()
```

    DataFrame[customer_id: string, review_id: string, product_id: string, product_parent: string, product_title: string, star_rating: int, helpful_votes: int, total_votes: int, verified_purchase: string, review_date: date, year: int, product_category: string, review_body: string]

## Number of Reviews


```pyspark3
df_new1.groupBy("product_category",'year').agg(F.count('review_id').alias('count_reviews')).sort("count_reviews",'year',ascending=False).show()
```

    +--------------------+----+-------------+
    |    product_category|year|count_reviews|
    +--------------------+----+-------------+
    |Digital_Ebook_Pur...|2014|      6253732|
    |Digital_Ebook_Pur...|2013|      4486392|
    |Digital_Ebook_Pur...|2015|      4145202|
    |               Books|2014|      3540838|
    |            Wireless|2015|      3000538|
    |               Books|2013|      2965970|
    |               Books|2015|      2860663|
    |            Wireless|2014|      2834284|
    |            Wireless|2013|      1767171|
    |               Books|2012|      1649719|
    |Digital_Ebook_Pur...|2012|      1526599|
    |               Books|2011|      1303082|
    |               Books|2010|      1120772|
    |               Books|2009|      1015572|
    |               Books|2008|       827721|
    |               Books|2007|       761037|
    |            Wireless|2012|       679881|
    |               Books|2006|       568401|
    |               Books|2005|       521047|
    |Digital_Ebook_Pur...|2011|       350138|
    +--------------------+----+-------------+
    only showing top 20 rows

## Number of Users


```pyspark3
df_new1.groupBy("product_category","year").agg(F.countDistinct('customer_id').alias('count_user')).sort("count_user","year",ascending=False).show()
```

    +--------------------+----+----------+
    |    product_category|year|count_user|
    +--------------------+----+----------+
    |Digital_Ebook_Pur...|2014|   2611998|
    |            Wireless|2015|   1982195|
    |Digital_Ebook_Pur...|2015|   1918246|
    |Digital_Ebook_Pur...|2013|   1879963|
    |            Wireless|2014|   1865523|
    |               Books|2014|   1859221|
    |               Books|2013|   1620951|
    |               Books|2015|   1548543|
    |                  PC|2014|   1344041|
    |                  PC|2015|   1291704|
    |            Wireless|2013|   1193473|
    |Digital_Video_Dow...|2015|    989863|
    |         Mobile_Apps|2014|    988668|
    |                  PC|2013|    982537|
    |               Books|2012|    951710|
    |Digital_Video_Dow...|2014|    931089|
    |         Mobile_Apps|2015|    858948|
    |Digital_Ebook_Pur...|2012|    786067|
    |               Books|2011|    752546|
    |         Mobile_Apps|2013|    743314|
    +--------------------+----+----------+
    only showing top 20 rows

## Average rating and median


```pyspark3
df_new1.groupBy("product_category","year").agg(F.mean('star_rating').alias('Average_star'),F.round(F.expr('percentile(star_rating, array(0.50))')[0]).alias('median')).sort("Average_star","year",ascending=False).show()
```

    +--------------------+----+------------------+------+
    |    product_category|year|      Average_star|median|
    +--------------------+----+------------------+------+
    |           Video_DVD|2015| 4.529892789948257|   5.0|
    |               Books|2015| 4.497385396322461|   5.0|
    |           Video_DVD|2014|   4.4854764769155|   5.0|
    |               Books|2014| 4.473267910025819|   5.0|
    |               Books|2013| 4.412500126434185|   5.0|
    |           Video_DVD|2013| 4.409201463118148|   5.0|
    |Digital_Ebook_Pur...|2015| 4.349694469381636|   5.0|
    |Digital_Ebook_Pur...|2014|   4.3328154291523|   5.0|
    |               Books|2012| 4.314683288487312|   5.0|
    |Digital_Ebook_Pur...|2013| 4.301701411281366|   5.0|
    |               Books|2007| 4.258166160120993|   5.0|
    |               Books|2011| 4.251138454832466|   5.0|
    |               Books|2010| 4.246952100873327|   5.0|
    |               Books|2009|4.2468244496697425|   5.0|
    |               Books|2008| 4.233279088001875|   5.0|
    |Digital_Video_Dow...|2014| 4.219862266015686|   5.0|
    |           Video_DVD|2012| 4.217564382025634|   5.0|
    |Digital_Ebook_Pur...|2012| 4.214260588405993|   5.0|
    |Digital_Video_Dow...|2013| 4.208396019980374|   5.0|
    |Digital_Video_Dow...|2015|  4.19841261059907|   5.0|
    +--------------------+----+------------------+------+
    only showing top 20 rows

## Percentile Length of the Review


```pyspark3
from pyspark.sql.functions import length
df2=df_new1.withColumn("length_of_review",length("review_body"))
```


```pyspark3
df2.show(5)
```

    +-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+-----------------+-----------+----+----------------+--------------------+----------------+
    |customer_id|     review_id|product_id|product_parent|       product_title|star_rating|helpful_votes|total_votes|verified_purchase|review_date|year|product_category|         review_body|length_of_review|
    +-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+-----------------+-----------+----+----------------+--------------------+----------------+
    |   10000113| RQ9E9A2ISWOQV|B0083XTNJK|     416505290|CrazyOnDigital Re...|          4|            0|          0|                Y| 2012-10-02|2012|              PC|I am very pleased...|             409|
    |   10000166|R3AAIE04RH1VKI|B00LDCJ190|      39152592|Leprechaun: Origi...|          3|            0|          0|                N| 2014-10-15|2014|       Video_DVD|while not an awes...|              91|
    |   10000185|R1437UFF5891NA|0061986380|     364246210|Extinction Agenda...|          5|            0|          0|                N| 2011-12-29|2011|           Books|Great book. I fin...|             295|
    |    1000021| RSI49CIWKMLHW|B00OB1IS08|     635789129|Note 4 Case, E LV...|          5|            0|          0|                Y| 2015-05-24|2015|        Wireless|So excited for th...|             218|
    |   10000221| RLMBSUF00TJIV|B004ZMH9OE|     519764619|TRIPP LITE USB 3....|          5|            0|          1|                Y| 2013-01-21|2013|              PC|we looked at a nu...|             130|
    +-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+-----------------+-----------+----+----------------+--------------------+----------------+
    only showing top 5 rows


```pyspark3
df2.groupby("product_category","year").agg(F.round(F.expr('percentile(length_of_review, array(0.1))')[0]).alias('%1'),\
                                                 F.round(F.expr('percentile(length_of_review, array(0.25))')[0]).alias('%25'),\
                                                 F.round(F.expr('percentile(length_of_review, array(0.50))')[0]).alias('%50'),\
                                                 F.round(F.expr('percentile(length_of_review, array(0.75))')[0]).alias('%75'),\
                                                 F.round(F.expr('percentile(length_of_review, array(0.9))')[0]).alias('%90'),\
                                                 F.round(F.expr('percentile(length_of_review, array(0.95))')[0]).alias('%95'))\
.show()

```

    +--------------------+----+-----+-----+------+------+------+------+
    |    product_category|year|   %1|  %25|   %50|   %75|   %90|   %95|
    +--------------------+----+-----+-----+------+------+------+------+
    |Digital_Video_Dow...|2014| 15.0| 45.0| 118.0| 192.0| 367.0| 563.0|
    |               Books|2012|137.0|208.0| 432.0| 985.0|1962.0|3162.0|
    |Digital_Ebook_Pur...|2015| 23.0| 73.0| 154.0| 340.0| 764.0|1257.0|
    |Digital_Ebook_Pur...|2008|119.0|225.0| 458.0| 996.0|1848.0|2724.0|
    |         Mobile_Apps|2015| 11.0| 24.0|  59.0| 125.0| 215.0| 315.0|
    |           Video_DVD|2015|  9.0| 19.0|  53.0| 149.0| 403.0| 769.0|
    |                  PC|2015| 12.0| 32.0|  82.0| 209.0| 467.0| 747.0|
    |Digital_Ebook_Pur...|2005|157.0|412.0|2468.0|3462.0|4284.0|5235.0|
    |            Wireless|2007|115.0|190.0| 357.0| 672.0|1233.0|1802.0|
    |           Video_DVD|2013|109.0|128.0| 180.0| 353.0| 825.0|1433.0|
    |           Video_DVD|2006|170.0|320.0| 639.0|1319.0|2549.0|3865.0|
    |           Video_DVD|2009|119.0|203.0| 420.0| 929.0|1903.0|3164.0|
    |         Mobile_Apps|2014| 20.0| 70.0| 119.0| 170.0| 278.0| 391.0|
    |Digital_Video_Dow...|2009|126.0|210.0| 384.0| 703.0|1218.0|1753.0|
    |         Mobile_Apps|2013|102.0|114.0| 136.0| 189.0| 298.0| 416.0|
    |               Books|2015| 12.0| 35.0| 100.0| 307.0| 822.0|1442.0|
    |Digital_Video_Dow...|2005|598.0|753.0|1203.0|1682.0|2546.0|3200.0|
    |            Wireless|2015| 11.0| 31.0|  79.0| 193.0| 409.0| 640.0|
    |               Books|2008|150.0|267.0| 521.0|1054.0|1970.0|3090.0|
    |Digital_Video_Dow...|2007|125.0|197.0| 356.0| 644.0|1082.0|1530.0|
    +--------------------+----+-----+-----+------+------+------+------+
    only showing top 20 rows

The percentile length in 2015  was 11 words with minimum percentile and highest length of the review is 1257 words

## Percentiles for number of reviews per product


```pyspark3
df_reviewcount = df_new1.groupBy("product_title","product_category","year").agg(F.count("review_id").alias('review_count'))
df_reviewcount.where(F.col('review_count')>1).show(5)

df_reviewcount.groupby("product_category","year").agg(F.round(F.expr('percentile(review_count, array(0.1))')[0]).alias('%1'),\
                                                 F.round(F.expr('percentile(review_count, array(0.25))')[0]).alias('%25'),\
                                                 F.round(F.expr('percentile(review_count, array(0.50))')[0]).alias('%50'),\
                                                 F.round(F.expr('percentile(review_count, array(0.75))')[0]).alias('%75'),\
                                                 F.round(F.expr('percentile(review_count, array(0.9))')[0]).alias('%90'),\
                                                 F.round(F.expr('percentile(review_count, array(0.95))')[0]).alias('%95'))\
.sort("product_category","year").show(truncate=False)
```

    +--------------------+--------------------+----+------------+
    |       product_title|    product_category|year|review_count|
    +--------------------+--------------------+----+------------+
    |Cable Matters Mic...|            Wireless|2014|          54|
    |Fateful Encounter...|               Books|2014|           5|
    |Blood Lust: Portr...|Digital_Ebook_Pur...|2012|          20|
    |       The Last Nude|               Books|2012|          40|
    |A Discovery of Wi...|Digital_Ebook_Pur...|2012|         416|
    +--------------------+--------------------+----+------------+
    only showing top 5 rows
    
    +----------------------+----+---+---+---+---+----+----+
    |product_category      |year|%1 |%25|%50|%75|%90 |%95 |
    +----------------------+----+---+---+---+---+----+----+
    |Books                 |2005|1.0|1.0|1.0|2.0|4.0 |7.0 |
    |Books                 |2006|1.0|1.0|1.0|2.0|4.0 |7.0 |
    |Books                 |2007|1.0|1.0|1.0|2.0|4.0 |7.0 |
    |Books                 |2008|1.0|1.0|1.0|2.0|4.0 |7.0 |
    |Books                 |2009|1.0|1.0|1.0|2.0|4.0 |7.0 |
    |Books                 |2010|1.0|1.0|1.0|2.0|4.0 |8.0 |
    |Books                 |2011|1.0|1.0|1.0|2.0|5.0 |8.0 |
    |Books                 |2012|1.0|1.0|1.0|2.0|5.0 |8.0 |
    |Books                 |2013|1.0|1.0|1.0|3.0|6.0 |10.0|
    |Books                 |2014|1.0|1.0|1.0|3.0|6.0 |11.0|
    |Books                 |2015|1.0|1.0|1.0|2.0|5.0 |10.0|
    |Digital_Ebook_Purchase|2005|1.0|1.0|1.0|1.0|1.0 |1.0 |
    |Digital_Ebook_Purchase|2006|1.0|1.0|1.0|1.0|1.0 |1.0 |
    |Digital_Ebook_Purchase|2007|1.0|1.0|1.0|1.0|1.0 |2.0 |
    |Digital_Ebook_Purchase|2008|1.0|1.0|1.0|1.0|2.0 |3.0 |
    |Digital_Ebook_Purchase|2009|1.0|1.0|1.0|2.0|3.0 |5.0 |
    |Digital_Ebook_Purchase|2010|1.0|1.0|1.0|2.0|4.0 |7.0 |
    |Digital_Ebook_Purchase|2011|1.0|1.0|1.0|3.0|6.0 |10.0|
    |Digital_Ebook_Purchase|2012|1.0|1.0|2.0|4.0|9.0 |16.0|
    |Digital_Ebook_Purchase|2013|1.0|1.0|2.0|5.0|14.0|28.0|
    +----------------------+----+---+---+---+---+----+----+
    only showing top 20 rows

 ## Identify week number (each year has 52 weeks) for each year and product category
## with most positive reviews (4 and 5 star).


```pyspark3
from pyspark.sql.functions import weekofyear
df_new1.withColumn('week_of_year',weekofyear("review_date")).filter(F.col("star_rating") >= 4).show(10)
```

    +-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+-----------------+-----------+----+--------------------+--------------------+------------+
    |customer_id|     review_id|product_id|product_parent|       product_title|star_rating|helpful_votes|total_votes|verified_purchase|review_date|year|    product_category|         review_body|week_of_year|
    +-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+-----------------+-----------+----+--------------------+--------------------+------------+
    |   10000113| RQ9E9A2ISWOQV|B0083XTNJK|     416505290|CrazyOnDigital Re...|          4|            0|          0|                Y| 2012-10-02|2012|                  PC|I am very pleased...|          40|
    |   10000185|R1437UFF5891NA|0061986380|     364246210|Extinction Agenda...|          5|            0|          0|                N| 2011-12-29|2011|               Books|Great book. I fin...|          52|
    |    1000021| RSI49CIWKMLHW|B00OB1IS08|     635789129|Note 4 Case, E LV...|          5|            0|          0|                Y| 2015-05-24|2015|            Wireless|So excited for th...|          21|
    |   10000221| RLMBSUF00TJIV|B004ZMH9OE|     519764619|TRIPP LITE USB 3....|          5|            0|          1|                Y| 2013-01-21|2013|                  PC|we looked at a nu...|           4|
    |   10000354|R33MI7K7D6LAGT|B00J19Y7GY|     664256134|A Wrong Turn Towa...|          5|            0|          0|                Y| 2015-05-13|2015|Digital_Ebook_Pur...|Awesome read! I r...|          20|
    |   10000371|R19KMA814XC67R|0345444051|     589013680|Childhood's End (...|          5|            0|          1|                N| 2011-12-18|2011|               Books|An awe-inspiring ...|          50|
    |   10000462| RKBF5R2I3TOJT|B009TGVUPA|     517637909|            The Mist|          5|            0|          0|                Y| 2015-08-17|2015|Digital_Video_Dow...|A good adaptation...|          34|
    |   10000470|R3NP56MW8B4FMA|0545615402|     160159764|Hogwarts Library ...|          4|            0|          1|                Y| 2014-01-08|2014|               Books|Got this set at a...|           2|
    |   10000579|R3OI1AQBGZZW33|B007D6J64K|     775471014|Zebra Print Anima...|          5|            0|          0|                Y| 2013-05-07|2013|            Wireless|it fits great, it...|          19|
    |   10000589| R7IPIFU04MZCK|B002FFT8Z6|     143462339|USB 2.0 High Spee...|          5|           24|         28|                Y| 2011-08-13|2011|                  PC|This item was as ...|          32|
    +-----------+--------------+----------+--------------+--------------------+-----------+-------------+-----------+-----------------+-----------+----+--------------------+--------------------+------------+
    only showing top 10 rows

## Detailed analysis of "Digital eBook Purchase" versus Books


```pyspark3
df_month=df_new1.withColumn("month", F.month("review_date").alias('month'))
product_category_pivot=['Digital_Ebook_Purchase','Books']
pivoted = df_month.groupBy("year","month").pivot("product_category",product_category_pivot)\
                    .agg(F.count("review_id").alias("Tot_Rev"),\
                         F.mean("star_rating").alias("Avg_star_rating"))\
                         .sort("year","month",ascending=False)
pivoted.show(n=5)
```

    +----+-----+------------------------------+--------------------------------------+-------------+---------------------+
    |year|month|Digital_Ebook_Purchase_Tot_Rev|Digital_Ebook_Purchase_Avg_star_rating|Books_Tot_Rev|Books_Avg_star_rating|
    +----+-----+------------------------------+--------------------------------------+-------------+---------------------+
    |2015|    8|                        578606|                     4.337671921825906|       347646|    4.479634455739459|
    |2015|    7|                        564277|                     4.341426285317318|       339311|     4.47737621238333|
    |2015|    6|                        534046|                    4.3503836748145295|       326946|    4.486331076079842|
    |2015|    5|                        574318|                     4.342918034956244|       327538|     4.48968974592261|
    |2015|    4|                        478123|                     4.352842260255207|       336509|    4.497927247116719|
    +----+-----+------------------------------+--------------------------------------+-------------+---------------------+
    only showing top 5 rows


```pyspark3
%%configure -f
{ "conf":{
"spark.pyspark.python": "python3",
"spark.pyspark.virtualenv.enabled": "true",
"spark.pyspark.virtualenv.type":"native",
"spark.pyspark.virtualenv.bin.path":"/usr/bin/virtualenv"
}}
```

    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>1</td><td>application_1588554627185_0009</td><td>pyspark3</td><td>idle</td><td><a target="_blank" href="http://ip-172-31-43-121.ec2.internal:20888/proxy/application_1588554627185_0009/">Link</a></td><td><a target="_blank" href="http://ip-172-31-35-148.ec2.internal:8042/node/containerlogs/container_1588554627185_0009_01_000001/livy">Link</a></td><td>✔</td></tr></table>


    SparkSession available as 'spark'.



Current session configs: <tt>{'conf': {'spark.pyspark.python': 'python3', 'spark.pyspark.virtualenv.enabled': 'true', 'spark.pyspark.virtualenv.type': 'native', 'spark.pyspark.virtualenv.bin.path': '/usr/bin/virtualenv'}, 'proxyUser': 'jovyan', 'kind': 'pyspark3'}</tt><br>



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>1</td><td>application_1588554627185_0009</td><td>pyspark</td><td>idle</td><td><a target="_blank" href="http://ip-172-31-43-121.ec2.internal:20888/proxy/application_1588554627185_0009/">Link</a></td><td><a target="_blank" href="http://ip-172-31-35-148.ec2.internal:8042/node/containerlogs/container_1588554627185_0009_01_000001/livy">Link</a></td><td>✔</td></tr></table>


## Producing the Graph for the Number of Reviews by year


```pyspark3
%%spark -o num_of_reviews_by_year
num_of_reviews_by_year=df_new1.groupBy("year").pivot("product_category",product_category_pivot)\
                    .agg(F.count("review_id").alias("Tot_Rev"))\
                         .sort("year",ascending=False)
```


```pyspark3
num_of_reviews_by_year.show()
```

    +----+----------------------+-------+
    |year|Digital_Ebook_Purchase|  Books|
    +----+----------------------+-------+
    |2015|               4609358|2860663|
    |2014|               6723895|3540838|
    |2013|               4569677|2965970|
    |2012|               1526599|1649719|
    |2011|                350138|1303082|
    |2010|                102515|1120772|
    |2009|                 31106|1015572|
    |2008|                  9607| 827721|
    |2007|                   508| 761037|
    |2006|                    36| 568401|
    |2005|                    19| 521047|
    +----+----------------------+-------+


```pyspark3
%%local
%matplotlib inline

import matplotlib.pyplot as plt
bar_width = 0.4
fig = plt.figure()
ax = fig.add_axes([0,0,1,1])
ax.bar(num_of_reviews_by_year.year,num_of_reviews_by_year.Digital_Ebook_Purchase,width=bar_width)
ax.bar(num_of_reviews_by_year.year+bar_width,num_of_reviews_by_year.Books,width=bar_width)
ax.set_ylabel('Number_reviews')
ax.set_xlabel('year')
ax.set_title('Number of reviews by product_category and year')
ax.legend(labels=['Digital_Ebook_Purchase', 'Books'])
```




    <matplotlib.legend.Legend at 0x7fc193f170b8>




![png](output_34_1.png)

Number of Reviews for the digital is higher in contrast to the books over the years.



## Producing the Graph for the  Average Stars

```pyspark3
%%spark -o Average_stars
Average_stars=df_new1.groupBy("year").pivot("product_category",product_category_pivot)\
                    .agg(F.mean("star_rating").alias("Avg_star_rating"))\
                         .sort("year",ascending=False)
```


```pyspark3
Average_stars.show()
```

    +----+----------------------+------------------+
    |year|Digital_Ebook_Purchase|             Books|
    +----+----------------------+------------------+
    |2015|     4.349695120231495| 4.497386095461088|
    |2014|     4.332816470215552| 4.473267910025819|
    |2013|     4.301702067782909| 4.412500463592012|
    |2012|     4.214259278304256|4.3146826823234745|
    |2011|     4.055552382203588| 4.251138454832466|
    |2010|    3.8219675169487393| 4.246951208631193|
    |2009|    3.7771169549283097| 4.246825434336512|
    |2008|     3.945872801082544| 4.233279088001875|
    |2007|     3.938976377952756| 4.258166160120993|
    |2006|     4.027777777777778| 4.196556656304264|
    |2005|    3.5789473684210527|  4.14809028744048|
    +----+----------------------+------------------+


```pyspark3
%%local
%matplotlib inline

import matplotlib.pyplot as plt
bar_width = 0.4
fig = plt.figure()
ax = fig.add_axes([0,0,1,1])
ax.bar(Average_stars.year,Average_stars.Digital_Ebook_Purchase,width=bar_width)
ax.bar(Average_stars.year+bar_width,Average_stars.Books,width=bar_width)
ax.set_ylabel('Average_stars')
ax.set_xlabel('year')
ax.set_title('Average_stars by product_category and year')
ax.legend(labels=['Digital_Ebook_Purchase', 'Books'])
```




    <matplotlib.legend.Legend at 0x7fc193db3908>




![png](output_37_1.png)

Average star for the printed books is higher in contrast to the digital  books over the years.

## Identifying Similar Products in Digital_Ebook_Purchase and Books


```pyspark3
from pyspark.sql.functions import lower,col,trim
df_compare=\
df_new1.select("customer_id","review_id","product_id","product_parent",\
               lower(trim(col('product_title'))).alias('product_title'),"star_rating","helpful_votes","total_votes",\
               "verified_purchase","review_body","review_date","year","product_category")\
.where((F.col('product_category')=='Digital_Ebook_Purchase') | (F.col('product_category')=='Books'))
```


```pyspark3
df_rating=df_compare.groupBy("product_title").pivot("product_category")\
                    .agg(F.round(F.mean("star_rating"),3).alias("avg_rating"))\
                    .sort("product_title",ascending=False)

```


```pyspark3
df_rating.where((F.col("Books").isNotNull()) & (F.col("Digital_Ebook_Purchase").isNotNull())\
               & (F.col("Books") != F.col("Digital_Ebook_Purchase"))).show()
```

    +--------------------+-----+----------------------+
    |       product_title|Books|Digital_Ebook_Purchase|
    +--------------------+-----+----------------------+
    |“there she is, mi...|4.667|                   1.0|
    |“the devil’s to p...|4.725|                 4.744|
    |‘sailor’ malan: b...|  3.0|                   4.0|
    |þéodisc geléafa "...|4.375|                   5.0|
    |óscar y las mujer...| 4.25|                   2.5|
    |ética y psicoanál...|  5.0|                   4.0|
    |éminence: cardina...|3.333|                 3.875|
    |álvar núñez cabez...|  4.5|                 4.333|
    |água viva (new di...|  4.8|                   4.5|
    |à la mod: my so-c...|  5.0|                   4.5|
    |¿te acostarías co...|  5.0|                 4.875|
    |¿quién fue steve ...|  4.0|                   1.0|
    |¿por qué ese idio...|  4.2|                   4.0|
    |¿muerta?... ¡pero...|  4.2|                 4.615|
    |¿fiestas judías o...|  5.0|                  4.75|
    |¿deberían los cri...|  3.0|                   2.0|
    |¿cómo salgo de mi...|  5.0|                 4.571|
    |¿cómo llego a fin...|  5.0|                 4.769|
    |¿cómo conquistar ...|  5.0|                   4.4|
    |¿como se dice...?...|  3.0|                   1.0|
    +--------------------+-----+----------------------+
    only showing top 20 rows

Average Rating of Digital_ebook_purchase and books varies .The printed book seems to have higher rating when compared to Digital book.

## Amazon reviews LDA topic modeling - for EMR 

## performing LDA for books with rating 1 and 2

```
from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import CountVectorizer, IDF,RegexTokenizer, Tokenizer
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import struct
import re
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import CountVectorizer
```

### Load Retail Data set

```
df = spark.read\
          .option("header", "true")\
          .option("inferSchema", "true")\
          .option("basePath", "hdfs:///hive/amazon-reviews-pds/parquet/")\
          .parquet("hdfs:///hive/amazon-reviews-pds/parquet/*")
```

```
df.printSchema()
root
 |-- marketplace: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- review_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- product_parent: string (nullable = true)
 |-- product_title: string (nullable = true)
 |-- star_rating: integer (nullable = true)
 |-- helpful_votes: integer (nullable = true)
 |-- total_votes: integer (nullable = true)
 |-- vine: string (nullable = true)
 |-- verified_purchase: string (nullable = true)
 |-- review_headline: string (nullable = true)
 |-- review_body: string (nullable = true)
 |-- review_date: date (nullable = true)
 |-- year: integer (nullable = true)
 |-- product_category: string (nullable = true)
```

## cleaning data

```
df_ml = df.filter((F.col("product_category")=="Digital_Ebook_Purchase") | (F.col("product_category")=="Books") \
                   & (F.col("year")==2015) \
                   & (F.col("review_date")<'2015-02-01')
                   & (F.col("star_rating")<3))
```

```
df1 = df_ml.withColumn('review_text', 
                       F.concat(F.col('review_headline'),F.lit(' '), F.col('review_body')))
corpus =df1.select('review_text')
corpus_df = corpus.withColumn("id", F.monotonically_increasing_id())
corpus_df = corpus_df.dropna()
corpus_df.persist()
print('Corpus size:', corpus_df.count())
corpus_df.show(5)
```

```
Corpus size: 19208757
+--------------------+----------+
|         review_text|        id|
+--------------------+----------+
|Two Stars Nice il...|8589934592|
|He thinks the boo...|8589934593|
|terrible book. No...|8589934594|
|Is this guy kiddi...|8589934595|
|The dumbing down ...|8589934596|
+--------------------+----------+
only showing top 5 rows
```

```
tokenizer = Tokenizer(inputCol="review_text", outputCol="words")
countTokens = udf(lambda words: len(words), IntegerType())
regexTokenizer = RegexTokenizer(inputCol="review_text", 
                                outputCol="words",pattern="\\w+", gaps=False)

tokenized_df = regexTokenizer.transform(corpus_df)
tokenized_df.select("review_text", "words") \
    .withColumn("tokens", countTokens(F.col("words"))).show()
```

```
+--------------------+--------------------+------+
|         review_text|               words|tokens|
+--------------------+--------------------+------+
|Two Stars Nice il...|[two, stars, nice...|    19|
|He thinks the boo...|[he, thinks, the,...|   115|
|terrible book. No...|[terrible, book, ...|    13|
|Is this guy kiddi...|[is, this, guy, k...|  1082|
|The dumbing down ...|[the, dumbing, do...|   179|
|Waste of money Ni...|[waste, of, money...|    27|
|Two Stars Much Ad...|[two, stars, much...|     7|
|One Star Allusion...|[one, star, allus...|    16|
|Distracting Writi...|[distracting, wri...|   126|
|One Star The book...|[one, star, the, ...|    10|
|very deceptive ti...|[very, deceptive,...|    38|
|Great for someone...|[great, for, some...|   184|
|Oh God, don; t we...|[oh, god, don, t,...|    18|
|Nice photography....|[nice, photograph...|    50|
|Two Stars interes...|[two, stars, inte...|     7|
|Lucifer with an U...|[lucifer, with, a...|    33|
|One Star This is ...|[one, star, this,...|    22|
|I wouldn't say I ...|[i, wouldn, t, sa...|   163|
|Amazon, you made ...|[amazon, you, mad...|    65|
|the book is a TOT...|[the, book, is, a...|    34|
+--------------------+--------------------+------+
only showing top 20 rows
```



## Stop words

```
stop_words = ['a', 'about', 'above', 'across', 'after', 'afterwards', 'again', 'against', 'all', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'amoungst', 'amount', 'an', 'and', 'another', 'any', 'anyhow', 'anyone', 'anything', 'anyway', 'anywhere', 'are', 'around', 'as', 'at', 'back', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'behind', 'being', 'below', 'beside', 'besides', 'between', 'beyond', 'bill', 'both', 'bottom', 'but', 'by', 'call', 'can', 'cannot', 'cant', 'co', 'computer', 'con', 'could', 'couldnt', 'cry', 'de', 'describe', 'detail', 'do', 'done', 'down', 'due', 'during', 'each', 'eg', 'eight', 'either', 'eleven', 'else', 'elsewhere', 'empty', 'enough', 'etc', 'even', 'ever', 'every', 'everyone', 'everything', 'everywhere', 'except', 'few', 'fifteen', 'fify', 'fill', 'find', 'fire', 'first', 'five', 'for', 'former', 'formerly', 'forty', 'found', 'four', 'from', 'front', 'full', 'further', 'get', 'give', 'go', 'had', 'has', 'hasnt', 'have', 'he', 'hence', 'her', 'here', 'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'however', 'hundred', 'i', 'ie', 'if', 'in', 'inc', 'indeed', 'interest', 'into', 'is', 'it', 'its', 'itself', 'keep', 'last', 'latter', 'latterly', 'least', 'less', 'ltd', 'made', 'many', 'may', 'me', 'meanwhile', 'might', 'mill', 'mine', 'more', 'moreover', 'most', 'mostly', 'move', 'much', 'must', 'my', 'myself', 'name', 'namely', 'neither', 'never', 'nevertheless', 'next', 'nine', 'no', 'nobody', 'none', 'noone', 'nor', 'not', 'nothing', 'now', 'nowhere', 'of', 'off', 'often', 'on', 'once', 'one', 'only', 'onto', 'or', 'other', 'others', 'otherwise', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'part', 'per', 'perhaps', 'please', 'put', 'rather', 're', 'same', 'see', 'seem', 'seemed', 'seeming', 'seems', 'serious', 'several', 'she', 'should', 'show', 'side', 'since', 'sincere', 'six', 'sixty', 'so', 'some', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhere', 'still', 'such', 'system', 'take', 'ten', 'than', 'that', 'the', 'their', 'them', 'themselves', 'then', 'thence', 'there', 'thereafter', 'thereby', 'therefore', 'therein', 'thereupon', 'these', 'they', 'thick', 'thin', 'third', 'this', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'to', 'together', 'too', 'top', 'toward', 'towards', 'twelve', 'twenty', 'two', 'un', 'under', 'until', 'up', 'upon', 'us', 'very', 'via', 'was', 'we', 'well', 'were', 'what', 'whatever', 'when', 'whence', 'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whither', 'who', 'whoever', 'whole', 'whom', 'whose', 'why', 'will', 'with', 'within', 'without', 'would', 'yet', 'you', 'your', 'yours', 'yourself', 'yourselves', '']
stop_words = stop_words + ['br','book','34']
```

```
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
tokenized_df1 = remover.transform(tokenized_df)
tokenized_df1.show(5)

stopwordList = stop_words

remover=StopWordsRemover(inputCol="filtered", outputCol="filtered_more" ,stopWords=stopwordList)
tokenized_df2 = remover.transform(tokenized_df1)
tokenized_df2.show(5)
```

```
+--------------------+----------+--------------------+--------------------+
|         review_text|        id|               words|            filtered|
+--------------------+----------+--------------------+--------------------+
|Two Stars Nice il...|8589934592|[two, stars, nice...|[two, stars, nice...|
|He thinks the boo...|8589934593|[he, thinks, the,...|[thinks, book, fu...|
|terrible book. No...|8589934594|[terrible, book, ...|[terrible, book, ...|
|Is this guy kiddi...|8589934595|[is, this, guy, k...|[guy, kidding, re...|
|The dumbing down ...|8589934596|[the, dumbing, do...|[dumbing, society...|
+--------------------+----------+--------------------+--------------------+
only showing top 5 rows

+--------------------+----------+--------------------+--------------------+--------------------+
|         review_text|        id|               words|            filtered|       filtered_more|
+--------------------+----------+--------------------+--------------------+--------------------+
|Two Stars Nice il...|8589934592|[two, stars, nice...|[two, stars, nice...|[stars, nice, ill...|
|He thinks the boo...|8589934593|[he, thinks, the,...|[thinks, book, fu...|[thinks, funny, n...|
|terrible book. No...|8589934594|[terrible, book, ...|[terrible, book, ...|[terrible, stars,...|
|Is this guy kiddi...|8589934595|[is, this, guy, k...|[guy, kidding, re...|[guy, kidding, re...|
|The dumbing down ...|8589934596|[the, dumbing, do...|[dumbing, society...|[dumbing, society...|
+--------------------+----------+--------------------+--------------------+--------------------+
only showing top 5 rows
```

## Vectorize

```
cv = CountVectorizer(inputCol="filtered_more", outputCol="features", vocabSize = 10000)
cvmodel = cv.fit(tokenized_df2)
featurized_df = cvmodel.transform(tokenized_df2)
vocab = cvmodel.vocabulary
featurized_df.select('filtered_more','features','id').show(5)

+--------------------+--------------------+----------+
|       filtered_more|            features|        id|
+--------------------+--------------------+----------+
|[stars, nice, ill...|(10000,[14,115,11...|8589934592|
|[thinks, funny, n...|(10000,[5,21,62,8...|8589934593|
|[terrible, stars,...|(10000,[14,161,80...|8589934594|
|[guy, kidding, re...|(10000,[0,2,3,4,5...|8589934595|
|[dumbing, society...|(10000,[5,8,11,13...|8589934596|
+--------------------+--------------------+----------+
only showing top 5 rows
```

##  Training DF

```
countVectors = featurized_df.select('features','id')
countVectors.persist()
print('Records in the DF:', countVectors.count())
Records in the DF: 19208757
```

```
lda = LDA(k=5, maxIter=10)
model = lda.fit(countVectors)
'\nll = model.logLikelihood(countVectors)\nlp = model.logPerplexity(countVectors)\nprint("The lower bound on the log likelihood of the entire corpus: " + str(ll))\nprint("The upper bound on perplexity: " + str(lp))\n\n# Describe topics.\ntopics = model.describeTopics(3)\nprint("The topics described by their top-weighted terms:")\ntopics.show(truncate=False)\n\n# Shows the result\ntransformed = model.transform(countVectors)\ntransformed.show(truncate=False)\n'
```

```
topics = model.describeTopics()   
topics_rdd = topics.rdd

topics_words = topics_rdd\
       .map(lambda row: row['termIndices'])\
       .map(lambda idx_list: [vocab[idx] for idx in idx_list])\
       .collect()

for idx, topic in enumerate(topics_words):
    print ("topic: ", idx)
    print ("----------")
    for word in topic:
       print (word)
    print ("----------")
    
    
    topic:  0
----------
story
love
read
characters
series
good
great
author
loved
reading
----------
topic:  1
----------
read
good
story
great
really
like
love
stars
time
didn
----------
topic:  2
----------
read
great
books
series
stars
story
reading
like
characters
loved
----------
topic:  3
----------
story
love
read
life
like
way
family
time
characters
written
----------
topic:  4
----------
read
good
great
author
like
reading
story
time
books
people
----------
```

## performing LDA for books with rating 3 and 4

 ## Cleaning the Data

```
df_ml = df.filter((F.col("product_category")=="Digital_Ebook_Purchase") | (F.col("product_category")=="Books") \
                   & (F.col("year")==2015) \
                   & (F.col("review_date")<'2015-02-01')
                   & (F.col("star_rating")>3))
                   
df1 = df_ml.withColumn('review_text', 
                       F.concat(F.col('review_headline'),F.lit(' '), F.col('review_body')))
corpus =df1.select('review_text')
corpus_df = corpus.withColumn("id", F.monotonically_increasing_id())
corpus_df = corpus_df.dropna()
corpus_df.persist()
print('Corpus size:', corpus_df.count())
corpus_df.show(5)
```

```
Corpus size: 19562171
+--------------------+-----------+
|         review_text|         id|
+--------------------+-----------+
|Excellent book. G...|94489280512|
|Five Stars Good i...|94489280513|
|Five Stars Love  ...|94489280514|
|Best author I've ...|94489280515|
|Five Stars Love t...|94489280516|
+--------------------+-----------+
only showing top 5 rows
```

```
tokenizer = Tokenizer(inputCol="review_text", outputCol="words")
countTokens = udf(lambda words: len(words), IntegerType())
regexTokenizer = RegexTokenizer(inputCol="review_text", 
                                outputCol="words",pattern="\\w+", gaps=False)
tokenized_df = regexTokenizer.transform(corpus_df)
tokenized_df.select("review_text", "words") \
    .withColumn("tokens", countTokens(F.col("words"))).show()
```

```
+--------------------+--------------------+------+
|         review_text|               words|tokens|
+--------------------+--------------------+------+
|Excellent book. G...|[excellent, book,...|    43|
|Five Stars Good i...|[five, stars, goo...|     4|
|Five Stars Love  ...|[five, stars, lov...|     4|
|Best author I've ...|[best, author, i,...|    39|
|Five Stars Love t...|[five, stars, lov...|    17|
|A book that both ...|[a, book, that, b...|   214|
|Five Stars Very s...|[five, stars, ver...|    11|
|good read for par...|[good, read, for,...|    35|
|Great book for a ...|[great, book, for...|    32|
|The ultimate body...|[the, ultimate, b...|    29|
|One of my favorit...|[one, of, my, fav...|    16|
|Nothing isn't not...|[nothing, isn, t,...|   119|
|Five Stars well w...|[five, stars, wel...|     6|
|Five Stars Wonder...|[five, stars, won...|     5|
|my child loved th...|[my, child, loved...|    15|
|      Five Stars :-)|       [five, stars]|     2|
|Four Stars Hard t...|[four, stars, har...|     9|
|my boys love thes...|[my, boys, love, ...|    13|
|Five Stars Daught...|[five, stars, dau...|    14|
|Five Stars This b...|[five, stars, thi...|    15|
+--------------------+--------------------+------+
only showing top 20 rows
```

```
stop_words = ['a', 'about', 'above', 'across', 'after', 'afterwards', 'again', 'against', 'all', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'amoungst', 'amount', 'an', 'and', 'another', 'any', 'anyhow', 'anyone', 'anything', 'anyway', 'anywhere', 'are', 'around', 'as', 'at', 'back', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'behind', 'being', 'below', 'beside', 'besides', 'between', 'beyond', 'bill', 'both', 'bottom', 'but', 'by', 'call', 'can', 'cannot', 'cant', 'co', 'computer', 'con', 'could', 'couldnt', 'cry', 'de', 'describe', 'detail', 'do', 'done', 'down', 'due', 'during', 'each', 'eg', 'eight', 'either', 'eleven', 'else', 'elsewhere', 'empty', 'enough', 'etc', 'even', 'ever', 'every', 'everyone', 'everything', 'everywhere', 'except', 'few', 'fifteen', 'fify', 'fill', 'find', 'fire', 'first', 'five', 'for', 'former', 'formerly', 'forty', 'found', 'four', 'from', 'front', 'full', 'further', 'get', 'give', 'go', 'had', 'has', 'hasnt', 'have', 'he', 'hence', 'her', 'here', 'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'however', 'hundred', 'i', 'ie', 'if', 'in', 'inc', 'indeed', 'interest', 'into', 'is', 'it', 'its', 'itself', 'keep', 'last', 'latter', 'latterly', 'least', 'less', 'ltd', 'made', 'many', 'may', 'me', 'meanwhile', 'might', 'mill', 'mine', 'more', 'moreover', 'most', 'mostly', 'move', 'much', 'must', 'my', 'myself', 'name', 'namely', 'neither', 'never', 'nevertheless', 'next', 'nine', 'no', 'nobody', 'none', 'noone', 'nor', 'not', 'nothing', 'now', 'nowhere', 'of', 'off', 'often', 'on', 'once', 'one', 'only', 'onto', 'or', 'other', 'others', 'otherwise', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'part', 'per', 'perhaps', 'please', 'put', 'rather', 're', 'same', 'see', 'seem', 'seemed', 'seeming', 'seems', 'serious', 'several', 'she', 'should', 'show', 'side', 'since', 'sincere', 'six', 'sixty', 'so', 'some', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhere', 'still', 'such', 'system', 'take', 'ten', 'than', 'that', 'the', 'their', 'them', 'themselves', 'then', 'thence', 'there', 'thereafter', 'thereby', 'therefore', 'therein', 'thereupon', 'these', 'they', 'thick', 'thin', 'third', 'this', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'to', 'together', 'too', 'top', 'toward', 'towards', 'twelve', 'twenty', 'two', 'un', 'under', 'until', 'up', 'upon', 'us', 'very', 'via', 'was', 'we', 'well', 'were', 'what', 'whatever', 'when', 'whence', 'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whither', 'who', 'whoever', 'whole', 'whom', 'whose', 'why', 'will', 'with', 'within', 'without', 'would', 'yet', 'you', 'your', 'yours', 'yourself', 'yourselves', '']
stop_words = stop_words + ['br','book','34']
```

```
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
tokenized_df1 = remover.transform(tokenized_df)
tokenized_df1.show(5)

stopwordList = stop_words

remover=StopWordsRemover(inputCol="filtered", outputCol="filtered_more" ,stopWords=stopwordList)
tokenized_df2 = remover.transform(tokenized_df1)
tokenized_df2.show(5)
```

```
+--------------------+-----------+--------------------+--------------------+
|         review_text|         id|               words|            filtered|
+--------------------+-----------+--------------------+--------------------+
|Excellent book. G...|94489280512|[excellent, book,...|[excellent, book,...|
|Five Stars Good i...|94489280513|[five, stars, goo...|[five, stars, goo...|
|Five Stars Love  ...|94489280514|[five, stars, lov...| [five, stars, love]|
|Best author I've ...|94489280515|[best, author, i,...|[best, author, ve...|
|Five Stars Love t...|94489280516|[five, stars, lov...|[five, stars, lov...|
+--------------------+-----------+--------------------+--------------------+
only showing top 5 rows

+--------------------+-----------+--------------------+--------------------+--------------------+
|         review_text|         id|               words|            filtered|       filtered_more|
+--------------------+-----------+--------------------+--------------------+--------------------+
|Excellent book. G...|94489280512|[excellent, book,...|[excellent, book,...|[excellent, gave,...|
|Five Stars Good i...|94489280513|[five, stars, goo...|[five, stars, goo...|[stars, good, inf...|
|Five Stars Love  ...|94489280514|[five, stars, lov...| [five, stars, love]|       [stars, love]|
|Best author I've ...|94489280515|[best, author, i,...|[best, author, ve...|[best, author, ve...|
|Five Stars Love t...|94489280516|[five, stars, lov...|[five, stars, lov...|[stars, love, gre...|
+--------------------+-----------+--------------------+--------------------+--------------------+
only showing top 5 rows
```

```
cv = CountVectorizer(inputCol="filtered_more", outputCol="features", vocabSize = 10000)
cvmodel = cv.fit(tokenized_df2)
featurized_df = cvmodel.transform(tokenized_df2)
vocab = cvmodel.vocabulary
featurized_df.select('filtered_more','features','id').show(5)
+--------------------+--------------------+-----------+
|       filtered_more|            features|         id|
+--------------------+--------------------+-----------+
|[excellent, gave,...|(10000,[0,10,18,1...|94489280512|
|[stars, good, inf...|(10000,[3,14,132]...|94489280513|
|       [stars, love]|(10000,[4,14],[1....|94489280514|
|[best, author, ve...|(10000,[0,8,11,30...|94489280515|
|[stars, love, gre...|(10000,[0,2,4,14,...|94489280516|
+--------------------+--------------------+-----------+
only showing top 5 rows
```

```
countVectors = featurized_df.select('features','id')
countVectors.persist()
print('Records in the DF:', countVectors.count())
Records in the DF: 19562171
```

```
lda = LDA(k=5, maxIter=10)
model = lda.fit(countVectors)
'\nll = model.logLikelihood(countVectors)\nlp = model.logPerplexity(countVectors)\nprint("The lower bound on the log likelihood of the entire corpus: " + str(ll))\nprint("The upper bound on perplexity: " + str(lp))\n\n# Describe topics.\ntopics = model.describeTopics(3)\nprint("The topics described by their top-weighted terms:")\ntopics.show(truncate=False)\n\n# Shows the result\ntransformed = model.transform(countVectors)\ntransformed.show(truncate=False)\n'
```

```
topics = model.describeTopics()   
topics_rdd = topics.rdd

topics_words = topics_rdd\
       .map(lambda row: row['termIndices'])\
       .map(lambda idx_list: [vocab[idx] for idx in idx_list])\
       .collect()

for idx, topic in enumerate(topics_words):
    print ("topic: ", idx)
    print ("----------")
    for word in topic:
       print (word)
    print ("----------")
    
    
    topic:  0
----------
story
characters
love
read
series
author
good
great
reading
books
----------
topic:  1
----------
read
good
great
story
really
like
stars
love
enjoyed
reading
----------
topic:  2
----------
read
series
books
great
stars
reading
love
loved
story
like
----------
topic:  3
----------
story
love
read
life
like
way
time
really
family
know
----------
topic:  4
----------
read
great
good
like
author
reading
story
time
people
books
----------
```

### Topic Modeling

*Topic Modeling* refers to the process of dividing a corpus of documents in two:

1. A list of the topics covered by the documents in the corpus

2. Several sets of documents from the corpus grouped by the topics they cover.

   From above star rating topic modelling using LDA we can conclude that topic modelling provides good approximation to number of stars given in the review

# Conclusion



There has been a consistent growth in the number of reviews as well as the reviewers over the time span considered in the analysis. This obviously shows that more and more buyers are relying on the reviews of the customers who have already bought the product and are also writing reviews. This implies that the review system is an important feature of the amazon online shopping system and improving the system will definitely enhance the shopping experience.



Most reviewers prefer to write concise reviews as opposed to very large or extremely short reviews. However, lengthy reviews are considered more helpful overall. Another interesting trend that was observed is that 5-star reviews tend to be more lengthy. This suggests that the detailed description of reviews should be promoted which can be done by possibly asking product specific questions when asking for review. For e.g. when writing reviews for a phone, the review system may ask for the battery consumption, camera quality and so on.



Higher star rating (4 to 5 stars) is pretty common across all the reviewers, and have a overall high helpfulness rating as well. 1-star rating is also quite frequent, but are relatively less helpful. This trend is more visible for infrequent reviewers. This might point that having a binary rating would be helpful, but having more precision in rating makes comparison between products easier. These are some of the interesting trends that were uncovered throughout the course of this project.
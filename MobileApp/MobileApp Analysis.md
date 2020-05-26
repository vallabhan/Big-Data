```python
from pyspark.sql import SparkSession
```


```python
spark=SparkSession.builder.appName('MobileAppAnalytics').getOrCreate()
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
MobileApp=spark.read.csv("D:\Big_data_learning\mobileAppData\AppleStore.csv",header=True,inferSchema=True,sep=',')
```


```python
MobileApp.show(2)
```

    +-----+---------+--------------------+----------+--------+-----+----------------+----------------+-----------+---------------+-----+-----------+------------+---------------+---------------+--------+-------+
    |sl.no|       id|          track_name|size_bytes|currency|price|rating_count_tot|rating_count_ver|user_rating|user_rating_ver|  ver|cont_rating| prime_genre|sup_devices.num|ipadSc_urls.num|lang.num|vpp_lic|
    +-----+---------+--------------------+----------+--------+-----+----------------+----------------+-----------+---------------+-----+-----------+------------+---------------+---------------+--------+-------+
    |    1|281656475|     PAC-MAN Premium| 100788224|     USD| 3.99|           21292|              26|        4.0|            4.5|6.3.5|         4+|       Games|             38|              5|      10|      1|
    |    2|281796108|Evernote - stay o...| 158578688|     USD|  0.0|          161065|              26|        4.0|            3.5|8.2.2|         4+|Productivity|             37|              5|      23|      1|
    +-----+---------+--------------------+----------+--------+-----+----------------+----------------+-----------+---------------+-----+-----------+------------+---------------+---------------+--------+-------+
    only showing top 2 rows
    
    


```python
MobileApp=MobileApp.drop('sl.no').withColumnRenamed('ipadSc_urls.num','Screenshot')\
.withColumnRenamed('lang.num','langnum')\
.withColumnRenamed('sup_devices.num','sup_devices_num')
```


```python
from pyspark.sql.functions import isnan, when, count, col
MobileApp.select([count(when(col(c).isNull(), c)).alias(c) for c in MobileApp.columns]).show()
```

    +---+----------+----------+--------+-----+----------------+----------------+-----------+---------------+---+-----------+-----------+---------------+----------+-------+-------+
    | id|track_name|size_bytes|currency|price|rating_count_tot|rating_count_ver|user_rating|user_rating_ver|ver|cont_rating|prime_genre|sup_devices_num|Screenshot|langnum|vpp_lic|
    +---+----------+----------+--------+-----+----------------+----------------+-----------+---------------+---+-----------+-----------+---------------+----------+-------+-------+
    |  0|         0|         0|       0|    0|               0|               0|          0|              0|  0|          0|          0|              0|         0|      0|      0|
    +---+----------+----------+--------+-----+----------------+----------------+-----------+---------------+---+-----------+-----------+---------------+----------+-------+-------+
    
    


```python
MobileApp.show(2)
```

    +---------+--------------------+----------+--------+-----+----------------+----------------+-----------+---------------+-----+-----------+------------+---------------+----------+-------+-------+
    |       id|          track_name|size_bytes|currency|price|rating_count_tot|rating_count_ver|user_rating|user_rating_ver|  ver|cont_rating| prime_genre|sup_devices_num|Screenshot|langnum|vpp_lic|
    +---------+--------------------+----------+--------+-----+----------------+----------------+-----------+---------------+-----+-----------+------------+---------------+----------+-------+-------+
    |281656475|     PAC-MAN Premium| 100788224|     USD| 3.99|           21292|              26|        4.0|            4.5|6.3.5|         4+|       Games|             38|         5|     10|      1|
    |281796108|Evernote - stay o...| 158578688|     USD|  0.0|          161065|              26|        4.0|            3.5|8.2.2|         4+|Productivity|             37|         5|     23|      1|
    +---------+--------------------+----------+--------+-----+----------------+----------------+-----------+---------------+-----+-----------+------------+---------------+----------+-------+-------+
    only showing top 2 rows
    
    


```python
MobileApp.printSchema()
```

    root
     |-- id: integer (nullable = true)
     |-- track_name: string (nullable = true)
     |-- size_bytes: long (nullable = true)
     |-- currency: string (nullable = true)
     |-- price: double (nullable = true)
     |-- rating_count_tot: integer (nullable = true)
     |-- rating_count_ver: integer (nullable = true)
     |-- user_rating: double (nullable = true)
     |-- user_rating_ver: double (nullable = true)
     |-- ver: string (nullable = true)
     |-- cont_rating: string (nullable = true)
     |-- prime_genre: string (nullable = true)
     |-- sup_devices_num: integer (nullable = true)
     |-- Screenshot: integer (nullable = true)
     |-- langnum: integer (nullable = true)
     |-- vpp_lic: integer (nullable = true)
    
    


```python
MobileApp=MobileApp.select('id','track_name','size_bytes','rating_count_ver','prime_genre','Screenshot','langnum')
```


```python
MobileApp.show(2,False)
```

    +---------+-------------------------+----------+----------------+------------+----------+-------+
    |id       |track_name               |size_bytes|rating_count_ver|prime_genre |Screenshot|langnum|
    +---------+-------------------------+----------+----------------+------------+----------+-------+
    |281656475|PAC-MAN Premium          |100788224 |26              |Games       |5         |10     |
    |281796108|Evernote - stay organized|158578688 |26              |Productivity|5         |23     |
    +---------+-------------------------+----------+----------------+------------+----------+-------+
    only showing top 2 rows
    
    

## Convert bytes to MB and GB in a new column


```python
MobileApp=MobileApp.withColumn('MB',(col('size_bytes')/100000)).withColumn('GB',(col('MB')/1000))
```


```python
MobileApp.show(2)
```

    +---------+--------------------+----------+----------------+------------+----------+-------+----------+------------------+
    |       id|          track_name|size_bytes|rating_count_ver| prime_genre|Screenshot|langnum|        MB|                GB|
    +---------+--------------------+----------+----------------+------------+----------+-------+----------+------------------+
    |281656475|     PAC-MAN Premium| 100788224|              26|       Games|         5|     10|1007.88224|        1.00788224|
    |281796108|Evernote - stay o...| 158578688|              26|Productivity|         5|     23|1585.78688|1.5857868800000001|
    +---------+--------------------+----------+----------------+------------+----------+-------+----------+------------------+
    only showing top 2 rows
    
    

## List top 10 trending apps


```python
MobileApp.sort('rating_count_ver',ascending=False).show(10,False)

```

    +----------+------------------------------------------------+----------+----------------+-----------------+----------+-------+----------+-------------------+
    |id        |track_name                                      |size_bytes|rating_count_ver|prime_genre      |Screenshot|langnum|MB        |GB                 |
    +----------+------------------------------------------------+----------+----------------+-----------------+----------+-------+----------+-------------------+
    |387428400 |Infinity Blade                                  |624107810 |177050          |Games            |5         |13     |6241.0781 |6.241078099999999  |
    |1045901853|Geometry Dash Meltdown                          |55445504  |117470          |Games            |5         |1      |554.45504 |0.55445504         |
    |416023011 |My Verizon                                      |150791168 |107245          |Utilities        |3         |2      |1507.91168|1.5079116799999999 |
    |597855590 |Real Basketball                                 |70336512  |94315           |Games            |5         |1      |703.36512 |0.7033651200000001 |
    |310738695 |Zillow Real Estate - Homes for Sale & for Rent  |132632576 |88478           |Lifestyle        |5         |1      |1326.32576|1.32632576         |
    |310633997 |WhatsApp Messenger                              |135044096 |73088           |Social Networking|0         |35     |1350.44096|1.3504409599999998 |
    |500116670 |Clear Vision (17+)                              |37879808  |69225           |Games            |5         |1      |378.79808 |0.37879808000000004|
    |318592730 |Guess My Age  Math Magic                       |767126    |68841           |Education        |0         |1      |7.67126   |0.00767126         |
    |531184261 |Trigger Fist                                    |144196155 |58269           |Games            |5         |1      |1441.96155|1.44196155         |
    |392988420 |Zappos: shop shoes & clothes, fast free shipping|70325248  |39452           |Shopping         |4         |1      |703.25248 |0.70325248         |
    +----------+------------------------------------------------+----------+----------------+-----------------+----------+-------+----------+-------------------+
    only showing top 10 rows
    
    

### Difference in the average number of screenshots displayed of highest and lowest rating apps


```python

min_max=MobileApp.select(F.expr('min(rating_count_ver)'),F.expr('max(rating_count_ver)')).first()


```


```python
MobileApp.filter((F.col('rating_count_ver')==(min_max[0]))).agg(F.avg('Screenshot').alias('Avgmin_Screenshot')).show()
MobileApp.filter((F.col('rating_count_ver')==(min_max[1]))).agg(F.avg('Screenshot').alias('Avgmax_Screenshot')).show()
```

    +------------------+
    | Avgmin_Screenshot|
    +------------------+
    |2.7623007623007623|
    +------------------+
    
    +-----------------+
    |Avgmax_Screenshot|
    +-----------------+
    |              5.0|
    +-----------------+
    
    

## What percentage of high rated apps support  multiple languages


```python
(MobileApp.filter(col('langnum')>1).count())*100/MobileApp.count()
```




    47.08906488814784



## Does length of app description contribute to the ratings?


```python
length_App=MobileApp.withColumn('length_of_AppName',F.length('track_name'))
```


```python
length_App.show(2,False)
```

    +---------+-------------------------+----------+----------------+------------+----------+-------+----------+------------------+-----------------+
    |id       |track_name               |size_bytes|rating_count_ver|prime_genre |Screenshot|langnum|MB        |GB                |length_of_AppName|
    +---------+-------------------------+----------+----------------+------------+----------+-------+----------+------------------+-----------------+
    |281656475|PAC-MAN Premium          |100788224 |26              |Games       |5         |10     |1007.88224|1.00788224        |15               |
    |281796108|Evernote - stay organized|158578688 |26              |Productivity|5         |23     |1585.78688|1.5857868800000001|25               |
    +---------+-------------------------+----------+----------------+------------+----------+-------+----------+------------------+-----------------+
    only showing top 2 rows
    
    


```python
percentiles=length_App.stat.approxQuantile('rating_count_ver',[0.25,0.50,0.75],0.0)
```


```python
df_25=length_App.filter((F.col('rating_count_ver')<(percentiles[0])))
df_25.agg(F.avg('length_of_AppName')).show()
```

    +----------------------+
    |avg(length_of_AppName)|
    +----------------------+
    |    21.913374913374913|
    +----------------------+
    
    


```python
df_50=length_App.filter((F.col('rating_count_ver')>=(percentiles[0]))&(F.col('rating_count_ver')<(percentiles[1])))
df_50.agg(F.avg('length_of_AppName')).show()
```

    +----------------------+
    |avg(length_of_AppName)|
    +----------------------+
    |    25.554727526781555|
    +----------------------+
    
    


```python
df_75=length_App.filter((F.col('rating_count_ver')>=(percentiles[1]))&(F.col('rating_count_ver')<(percentiles[2])))
df_75.agg(F.avg('length_of_AppName')).show()
```

    +----------------------+
    |avg(length_of_AppName)|
    +----------------------+
    |     27.64095449500555|
    +----------------------+
    
    


```python
df_100=length_App.filter(F.col('rating_count_ver')>=(percentiles[2]))
df_100.agg(F.avg('length_of_AppName')).show()
```

    +----------------------+
    |avg(length_of_AppName)|
    +----------------------+
    |    26.511911357340722|
    +----------------------+
    
    


```python

```

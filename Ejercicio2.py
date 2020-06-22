from pyspark.sql import SparkSession 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
from array import array
import pandas as pd
import csv
from pyspark.sql import types as T
from pyspark.sql.window import Window 
from pyspark.sql.functions import col   
import pyspark.sql.functions
import os




scSpark = SparkSession \
    .builder \
    .appName("Ejercicio 2") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sdfData = scSpark.read.csv("demo.csv", header=True, sep=",")
sdfData.createOrReplaceTempView("demo")
sdfData.show()



country = scSpark.sql("SELECT distinct country as country from demo")
#country.collect()
country = country.toPandas()
country['country_id'] = country.index
countryDict = country.set_index('country_id')['country'].to_dict()
type(countryDict)



with open('dict.csv', 'w') as f:  
    [f.write('{0},{1}\n'.format(key, value)) for key, value in countryDict.items()]


sdfDict = scSpark.read.csv("dict.csv", header=False, sep=",")

sdfDict.createOrReplaceTempView("dictionario")

country = scSpark.sql("SELECT count(1) from demo")
countryInner = scSpark.sql("SELECT count(1) from demo INNER JOIN dictionario ON demo.country = dictionario._c1")
country.show()
countryInner.show()

country = scSpark.sql("SELECT count(1) from demo")
countryInner = scSpark.sql("SELECT count(1) from demo LEFT JOIN dictionario ON demo.country = dictionario._c1")
country.show()
countryInner.show()


countryInner = scSpark.sql("SELECT * from demo LEFT JOIN dictionario ON demo.country = dictionario._c1 where _c1 is null")

from pyspark.sql import SparkSession

"""
Fiding spark liberies to read S3
1 - Go to https://mvnrepository.com/
2 - Search for hadoop-aws
3 - Search for your spark version and copy maven's <groupId>:<artifactId>:<version>
    Ex: org.apache.hadoop:hadoop-aws:3.3.0
4 - Reapet steps 2 and 3 for Apache Hadoop Common
    Ex: org.apache.hadoop:hadoop-common:3.3.0
"""

spark = SparkSession.builder.\
    config('spark.master','local').\
    config('spark.app,name', 'S3app').\
    config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.2.1,org.apache.hadoop:hadoop-common:3.2.1').\
    getOrCreate()

spark

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key','XXXXXXXXXXXXXx')
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key','XXXXXXXXXXXXXXX')

#df1 = spark.read.format('csv').load('s3a://databricks-exercise/products.csv')

df_products = spark.read.format('csv').load('s3://databricks-exercise/products.csv',
                                           header='true', 
                                           inferSchema='true')
df_products.show()

df_sellers = spark.read.format('csv').load('s3://databricks-exercise/sellers.csv',
                                           header='true', 
                                           inferSchema='true')
df_sellers.show()

df_sales = spark.read.format('csv').load('s3://databricks-exercise/sales.csv',
                                           header='true', 
                                           inferSchema='true')
df_sales.show()
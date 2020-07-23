from pyspark.sql import SparkSession

if __name__ == '__main__' :
    spark = SparkSession.builder \
        .main("local") \
        .appName("Word Count") \
        .getOrCreate()

    jdbcHostname='localhost'
    jdbcDatabase='test'
    jdbcPort=3306
    jdbcUrl="jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
#    df = spark.read.jdbc(url=jdbcUrl, table="sample", lowerBound=1, upperBound=100000, numPartitions=100)
    df=spark.read.format("jdbc").options(driver = 'com.mysql.jdbc.Driver',url=jdbcUrl, dbtable="sample" ,user='myuser',password='Student123@').load()
    df.printSchema()
    df.show()

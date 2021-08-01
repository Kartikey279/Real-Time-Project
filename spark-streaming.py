import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# udf1

def get_total_item_cnt(items):
        total_cnt = 0
        for item in items:
                total_cnt = total_cnt + item['quantity']
        return total_cnt

# udf2

def get_total_cost(items,type):
        total_cost = 0
        for item in items:
                total_cost = total_cost + ( item['quantity'] * item['unit_price'] )
    if type == 'RETURN':
        total_cost = total_cost * -1
        return total_cost

# udf3

def check_order(type):
        if type == "ORDER":
                return 1
        else:
                return 0
# udf4

def check_return(type):
        if type == "RETURN":
                return 1
        else:
                return 0

spark=SparkSession.builder.appName('orders').getOrCreate()
spark.sparkContext.setLogLevel('error')

lines=spark.readStream.format('kafka') \
        .option("kafka.bootstrap.servers","18.211.252.152:9092") \
        .option("startingOffsets","latest") \
        .option("subscribe","real-time-project") \
        .load()

# Define schema

jsonSchema=StructType() \
        .add("invoice_no",LongType()) \
        .add("country",StringType()) \
        .add("timestamp",TimestampType()) \
        .add("type",StringType()) \
        .add("items",ArrayType(StructType([StructField("SKU",StringType()), \
                                        StructField("title",StringType()), \
                                           StructField("unit_price",DoubleType()), \
                                           StructField("quantity",IntegerType()) \
                                          ])))


orderStream=lines.select(from_json(col("value").cast("string"),jsonSchema).alias("data")).select("data.*")

#udf

add_total_item_count=udf(get_total_item_cnt,IntegerType())
add_total_cost=udf(get_total_cost,DoubleType())
returns=udf(check_return,IntegerType())
orders=udf(check_order,IntegerType())

#calc additional columns

expandedOrderStream=orderStream \
        .withColumn("total_items",add_total_item_count(orderStream.items)) \
        .withColumn("total_cost",add_total_cost(orderStream.items,orderStream.type)) \
        .withColumn("is_return",returns(orderStream.type)) \
        .withColumn("is_order",orders(orderStream.type))

#write to console

query=expandedOrderStream \
        .select("invoice_no","country","timestamp","total_cost","total_items","is_order","is_return") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate","false") \
        .start()


# calculate time based KPIs

timekpiDF = expandedOrderStream \
    .withWatermark("timestamp","1 minute") \
    .groupBy(window("timestamp","1 minute","1 minute")) \
    .agg(count("invoice_no").alias("OPM"), \
    sum("total_cost").alias("total_sales_volume"), \
    (sum("is_return")/(sum("is_order") + sum("is_return"))).alias("rate_of_return"), \
    (sum("total_cost")/(sum("is_order") + sum("is_return"))).alias("avg_trans_size")) \
    .select("window","OPM","total_sales_volume","avg_trans_size","rate_of_return")

# write time based KPIs

timedQuery = timekpiDF \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("truncate","false") \
    .option("path","/tmp/tq1") \
    .option("checkpointLocation","/tmp/tqcp1") \
    .trigger(processingTime="1 minute") \
        .start()

#calculate time-country kpis

timecntryDF = expandedOrderStream \
    .withWatermark("timestamp","1 minute") \
    .groupBy(window("timestamp","1 minute","1 minute"),"country") \
    .agg(count("invoice_no").alias("OPM"), \
        sum("total_cost").alias("total_sales_volume") \
        ,(sum("is_return")/(sum("is_order") + sum("is_return"))).alias("rate_of_return")) \
        .select("window","country","OPM","total_sales_volume","rate_of_return")

# write time-cntry kpis

cntryQuery = timecntryDF \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("truncate","false") \
        .option("path","/tmp/ctq1") \
        .option("checkpointLocation","/tmp/ctqcp1") \
        .start()


query.awaitTermination()
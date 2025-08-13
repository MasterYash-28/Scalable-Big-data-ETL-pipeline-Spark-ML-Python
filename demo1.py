
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,lower , col ,regexp_replace


# create spark session
spark = SparkSession.builder\
    .appName("project")\
    .getOrCreate()

# create dataframe and extract 
df = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(r"output2.csv")\
        
df.printSchema()


#Transform operations

df=df.na.drop()




df=df.drop_duplicates()




cols_for_regex=["title","text","subject","Target"]

for c in cols_for_regex :
    df=df.withColumn(c,regexp_replace(col(c),"[^a-zA-Z\s]",""))




cols_for_lower=["title","text","subject","date","Target"]

for c in cols_for_lower :
   
   df=df.withColumn(c,lower(col(c)))




df = df.withColumn("label",when(df["Target"] == "fake",1).otherwise(0))




df = df.select("title", "label")




df.show(5)

#Save the transformation 

df.coalesce(1).write \
    .option("header", True) \
    .mode("overwrite") \
    .csv("cleaned_fake_news")


# close the spark session
spark.stop()

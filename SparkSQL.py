from pyspark.sql.types import *


if __name__ == '__main__':

	usrRDD = sc.textFile("user2.csv")
	usrRDD = sc.textFile("user2.csv", use_unicode = False)

	schema = StructType([StructField('ID', IntegerType(), True),
                     StructField('Reputation', IntegerType(), True),
                     StructField('CreationDate', StringType(), True),
                     StructField('DisplayName', StringType(), True),
                     StructField('LateAccessDate', StringType(), True),
                     StructField('View', IntegerType(), True),
                     StructField('UpVotes', IntegerType(), True),
                     StructField('DownVotes', IntegerType(), True),
                     StructField('AccountID', IntegerType(),True)])

	usrSplited = usrRDD.map(lambda u: u.split(','))

	usrTyped = usrSplit.map(lambda u: [int(u[0]),int(u[1]),u[2],u[3],u[4],int(u[5]),int(u[6]),int(u[7]),int(u[8])])
	
	usr = usrTyped.map(lambda u: (u[0], u))
	
### The code below doesn't work.
###	RDD = usr.sortByKey(True)

	df = sqlContext.createDataFrame(usr, schema)
	df.createOrReplaceTempView("usr")
	results = spark.sql("SELECT Reputation, DisplayName FROM usr ORDER BY Reputaion")

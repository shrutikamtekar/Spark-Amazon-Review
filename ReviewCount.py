from pyspark import SparkConf, SparkContext
import json

# configuration part
conf = SparkConf()
conf.setMaster("spark://shruti-Inspiron-3521:7077")# set to your spark master url
conf.setAppName("Review Count")
sc = SparkContext(conf = conf)

# finds all review with words greater than 100
def ReviewCount(line):
	jsonRecord = json.loads(line)  #converts to json format
	reviewString = jsonRecord.get("reviewText") # gets review text
	if(len(reviewString.split())>100): # gets reviews more than 100 words
		reviewID = jsonRecord.get("reviewerID")
		overallInt = jsonRecord.get("overall")
		op = (overallInt,reviewID) # returns overall and reviewid if words greater than 100
		return op
	return None # returns none if words not greater than 100


review = sc.textFile("file:///home/shruti/hadoop_spark_scala/review.data") # read a local file starting with prefix file://
review = review.map(ReviewCount) # calls tha map function
review = review.filter(lambda x:x!=None) # removes none from the rdd

data = review.combineByKey(lambda v:[str(v)],lambda x,y:x+[str(y)],lambda x,y:x+y).collect() # groups review by the overall rating
data = sc.parallelize(data) # converts list to rdd
data.saveAsTextFile("file:///home/shruti/hadoop_spark_scala/Review_Count_Output") # save file to a local path, starting with prefix file://

from pyspark import SparkConf, SparkContext
import json
import os

# configuration part
conf = SparkConf()
conf.setMaster("spark://shruti-Inspiron-3521:7077")# set to your spark master url
conf.setAppName("MusicReview")
sc = SparkContext(conf = conf)

# filter meta.data for only music category data 
def filterCategory(meta):
	jsonRecord = json.loads(meta) #converts to json format
	if(jsonRecord.get("categories")=="Music"):
		return (jsonRecord.get("asin"),None) # if music category
	return None # if not music category

#filters review.data for asin and review text
def filterReview(review):
	jsonRecord = json.loads(review)
	reviewText = jsonRecord.get("reviewText") #extracts the json data
	return (jsonRecord.get("asin"),reviewText)


review = sc.textFile("file:///home/shruti/hadoop_spark_scala/review.data") # read a local file starting with prefix file://
review = review.map(filterReview) #maps to filter review.data

meta = sc.textFile("file:///home/shruti/hadoop_spark_scala/meta.data") # read a local file starting with prefix file://
meta = meta.map(filterCategory) # maps to filter review.data
meta = meta.filter(lambda x:x!=None) # removes none music data leaving only music category reviews

joined = sc.parallelize(meta.leftOuterJoin(review).collect()) # joins table based on music category 
joined.saveAsTextFile("file:///home/shruti/hadoop_spark_scala/music_joined_output") # outputs intermediate data
joined = joined.filter(lambda x:x[1][1]!=None) # eliminates data from meta.data of music category that does not have review
joined = joined.map(lambda x:(str(x[0]),len(x[1][1].split(  )))) #counts the no of words in review text for music category reviews 

totalWords = joined.map(lambda x : x[1]).sum() # counts total number of words for all reviews in music category
totalReview = joined.count() # counts total number of music reviews
avgReviews = float(totalWords/totalReview) # calculates average number of reviews of music category

complete_name = os.path.join( '/home/shruti/hadoop_spark_scala/','AverageMusicReview.txt') # write the average to a local file directory
with open(complete_name, "w") as f:	
	f.write('Total words '+str(totalWords)+' Total Reviews '+str(totalReview)+' Average Reviews '+str(avgReviews))
	f.close()


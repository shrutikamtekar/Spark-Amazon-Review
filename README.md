# Spark-Amazon-Review
##Introduction
This project is done to give overview and have hands on experience on Spark. Written a spark program that analyzez real product review of amazon data.
The data is present in meta.data and review.data file that is used as a input

###Installing spark:
1.	Download and extract from the site
2.	Start spark by starting the master and slave node

###Design Description for calculating all reviews with  more than 100 words and grouping by overall:
1.	Take in input data.
2.	Create a map function called ReviewCount, this function takes in as input the text file, converts it into json format.
3.	Using json.loads() reads the data and using json.get() to get ReviewText as a string. 
4.	The word is extracted from the json string file using split() function and calculates word length.
5.	If the words are greater than or equal to 100 then overall and reviewID is returned else none is returned.
6.	The output of the map function is stored in review.
7.	Filter function is used on the review to remove all the reviews that do not have 100 words i.e to filter out none from the RDD.
8.	Combine by key is used to group all the reviews for every category
9.	The output is then written to a text file.

###Design Description for calculating average number of words in each review for reviews belonging to music category:
1.	Takes input data of the meta file and sent to a map function called filterCategory, this function takes in as input the text file, converts it into json format and returns music category data.
2.	Using json.loads() reads data and using json.get() to get all asin id’s that belong to music category. 
3.	If the record belong to music category then a tuple of asin and none is returned else none is returned.
4.	The meta RDD is then filtered to remove all the data that does not belong to music category ie remove none from the RDD.
5.	The input of review file is taken and sent for filterReview map function, this function takes input and reutrns asin and review text.
6.	The output of the map function is stored in review.
7.	Meta and Review RDD are joined using left outer join to get all the review text that belong to music category. 
8.	The joined output is saved on local directory.
9.	Joined RDD is then mapped to create a count of words for all music reviews.
10.	Total number of words are calculated by summing all the music reviews words.
11.	Total number of music reviews are calculated
12.	The average is calculated by dividing total number of words and total review.
13.	The output of written to a text file on local directory.


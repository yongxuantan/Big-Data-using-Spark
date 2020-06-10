# Big-Data-using-Spark

### A repository to describe algorithms and Scala implementations learned during the Big Data Management course.

##### -------------------- PageRank.scala Program details --------------------
Page Rank for Airports: compute the page rank of each node (airport) based on number of inlinks and outlinks.<br/>
Data: [Bureau of Transportation](https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236)<br/>
Algorithm: [Page Rank with Damping Factor](https://en.wikipedia.org/wiki/PageRank)
```
Input Parameters:
  1. Input file path: csv file with 2 columns 
    i. origin
    ii. destination
  2. Number of iterations
  3. Output file path: destinations listed in decresing order based on page rank value
```

##### -------------------- TweetProcessing.scala Program details --------------------
Tweet processing: use a set of Tweets about US airlines and examine their sentiment polarity as either “positive”, “neutral”, or “negative” by using logistic regression classifier.<br/>
Kaggle Data: [twitter-airline-sentiment](https://www.kaggle.com/crowdflower/twitter-airline-sentiment)
```
Input Parameters:
  1. Input file path: make sure csv file has these 3 columns
    i. tweet_id
    ii. airline_sentiment
    iii. text
  2. Output file path: for the classification metrics
Pre-processing:
  1. Remove stop-words
  2. Tokenize sentence into words
  3. Convert words to term-frequency vectors
  4. Convert label to numeric form
```

##### -------------------- TopicModeling.scala Program details --------------------
Topic modeling: perform topic analysis on a classic book, then output the 5 most important topics from the book.<br/>
Data: [Gutenberg project](http://www.gutenberg.org)
```
Input Parameters:
  1. Input file path
  2. Output file path
```

##### -------------------- GraphX.scala Program details --------------------
Analyze social network using GraphX.<br/>
Data: [SNAP repository](https://snap.stanford.edu/data/#socnets)
```
Input Parameters:
  1. Input file path
  2. Output file path
Queries:
  1. top 5 nodes with the highest outdegree and their counts
  2. top 5 nodes with the highest indegree and their counts
  3. top 5 nodes with the highest PageRank values
  4. top 5 components with the largest connected components
  5. top 5 vertices with the largest triangle count
```


##### ============================================================
built.sbt file:
```
name := "Project"
version := "0.1"
scalaVersion := "2.11.8"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"
```

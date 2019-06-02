from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


comment_file = "./data/comments-minimal.json.bz2"
submission_file = "./data/submissions.json.bz2"
label_file = "./data/labeled_data.csv"






def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

    # task 1 load file
    # comments = sqlContext.read.json(comment_file)
    # submissions = sqlContext.read.json(submission_file)
    # labels = sqlContext.read.json(label_file)
    #
    # comments.write.parquet("comments.parquet")
    # submissions.write.parquet("submissions.parquet")
    # labels.write.parquet("labels.parquet")



    # task 2
    try:
        comments = context.read.parquet("comments").sample(False, 0.2, None)
    except:
        comments = context.read.json("comments-minimal.json.bz2")
        comments.write.parquet("comments.parquet")
    try:
        submissions = context.read.parquet("submissions").sample(False, 0.2, None)
    except:
        submissions = context.read.json("submissions.json.bz2")
        submissions.write.parquet("submissions.parquet")
    try:
        labels = context.read.parquet("labels").sample(False, 0.2, None)
    except:
        labels = context.read.format('csv').options(header='true', inferSchema='true').load("./data/labeled_data.csv")
        labels.write.parquet("labels.parquet")

    comments.show()
    submissions.show()




if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)


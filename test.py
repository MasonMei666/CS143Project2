from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, BooleanType
from pyspark.ml.feature import CountVectorizer

from cleantext import sanitize

comment_file = "./data/comments-minimal.json.bz2"
submission_file = "./data/submissions.json.bz2"
label_file = "./data/labeled_data.csv"


def preprocess(comment):
    _, unigrams, bigrams, trigrams = sanitize(comment)
    processed_comment = unigrams.split() + bigrams.split() + trigrams.split()
    return processed_comment

res = preprocess("And now In the negatives. This account has been dumping  propaganda like mad in the last few hours.\n\nIt's morning somewhere though")
print(res)
# def main(context):
#     """Main function takes a Spark SQL context."""
#     # YOUR CODE HERE
#     # YOU MAY ADD OTHER FUNCTIONS AS NEEDED
#
#     # task 1 load file
#     # comments = sqlContext.read.json(comment_file)
#     # submissions = sqlContext.read.json(submission_file)
#     # labels = context.read.format('csv').options(header='true', inferSchema='true').load(label_file)
#
#     # comments.write.parquet("comments.parquet")
#     # submissions.write.parquet("submissions.parquet")
#     # labels.write.parquet("labels.parquet")
#
#
#
#
#     try:
#         comments = context.read.parquet("comments").sample(False, 0.2, None)
#     except:
#         comments = context.read.json(comment_file)
#         comments.write.parquet("comments")
#     try:
#         submissions = context.read.parquet("submissions").sample(False, 0.2, None)
#     except:
#         submissions = context.read.json(submission_file)
#         submissions.write.parquet("submissions")
#     try:
#         labels = context.read.parquet("labels").sample(False, 0.2, None)
#     except:
#         labels = context.read.format('csv').options(header='true', inferSchema='true').load(label_file)
#         labels.write.parquet("labels")
#
#
#
#     # submissions = context.read.parquet("submissions.parquet")
#     # labels = context.read.parquet("labels.parquet")
#
#     # comments = context.read.parquet("comments")
#     # submissions = context.read.parquet("submissions")
#     # labels = context.read.parquet("labels")
#
#     # comments.show()
#     # submissions.show()
#     # labels.show()
#
#     # task 2
#     comments.createOrReplaceTempView("comments")
#     labels.createOrReplaceTempView("labels")
#     labeled_comments = context.sql(
#         "SELECT labels.Input_id, labels.labeldem, labels.labelgop, labels.labeldjt, body FROM comments JOIN labels ON id=Input_id")
#     labeled_comments.show()
#
#     labeled_comments.createOrReplaceTempView("labeled_comments")
#     context.registerFunction("sanitize", preprocess(), ArrayType(StringType()))
#
#
#
# if __name__ == "__main__":
#     conf = SparkConf().setAppName("CS143 Project 2B")
#     conf = conf.setMaster("local[*]")
#     sc = SparkContext(conf=conf)
#     sqlContext = SQLContext(sc)
#     sc.addPyFile("cleantext.py")
#     main(sqlContext)


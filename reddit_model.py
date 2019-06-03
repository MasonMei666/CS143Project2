from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, BooleanType, DoubleType
from pyspark.ml.feature import CountVectorizer
# Bunch of imports (may need more)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidatorModel, CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from cleantext import sanitize

comment_file = "./data/comments-minimal.json.bz2"
submission_file = "./data/submissions.json.bz2"
label_file = "./data/labeled_data.csv"


def preprocess(comment):
    _, unigrams, bigrams, trigrams = sanitize(comment)
    processed_comment = unigrams.split() + bigrams.split() + trigrams.split()
    return processed_comment


def subString(str):
    return str[3:]


# def getProbability(problist):
#     return problist[1]

# states reference from professor
states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware',
          'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas',
          'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi',
          'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
          'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island',
          'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
          'West Virginia', 'Wisconsin', 'Wyoming']


def check_state(state):
    return state in states


def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

    # task 1 load file
    # comments = sqlContext.read.json(comment_file)
    # submissions = sqlContext.read.json(submission_file)
    # labels = context.read.format('csv').options(header='true', inferSchema='true').load(label_file)

    # comments.write.parquet("comments.parquet")
    # submissions.write.parquet("submissions.parquet")
    # labels.write.parquet("labels.parquet")





    try:
        comments = context.read.parquet("comments")#.sample(False, 0.02, None)
    except:
        comments = context.read.json(comment_file)
        comments.write.parquet("comments")
    try:
        submissions = context.read.parquet("submissions")#.sample(False, 0.02, None)
    except:
        submissions = context.read.json(submission_file)
        submissions.write.parquet("submissions")
    try:
        labels = context.read.parquet("labels")#.sample(False, 0.2, None)
    except:
        labels = context.read.format('csv').options(header='true', inferSchema='true').load(label_file)
        labels.write.parquet("labels")



    # submissions = context.read.parquet("submissions.parquet")
    # labels = context.read.parquet("labels.parquet")

    # comments = context.read.parquet("comments")
    # submissions = context.read.parquet("submissions")
    # labels = context.read.parquet("labels")

    # comments.show()
    # submissions.show()
    # labels.show()

    # task 2
    comments.createOrReplaceTempView("comments")
    labels.createOrReplaceTempView("labels")
    query_2 = "SELECT labels.Input_id, labels.labeldem, labels.labelgop, labels.labeldjt, body FROM comments JOIN labels ON id=Input_id"
    labeled_comments = context.sql(query_2)
    # labeled_comments.show()

    # task 4
    labeled_comments.createOrReplaceTempView("labeled_comments")
    context.registerFunction("process", preprocess, ArrayType(StringType()))

    # task 5
    query_5 = "SELECT Input_id, labeldjt, process(body) AS comment FROM labeled_comments"
    combined_grams = context.sql(query_5)
    # combined_grams.show()

    # task 6A
    # minDF specifies the minimum number (or fraction if < 1.0) of documents a term must appear in to be included in the vocabulary
    cv = CountVectorizer(inputCol="comment", outputCol="features", minDF=10, binary=True)
    model = cv.fit(combined_grams)
    result = model.transform(combined_grams)
    # result.show()

    # # task 6B
    # result.createOrReplaceTempView("result")
    # query_6_pos = "SELECT *, IF(labeldjt = 1, 1, 0) AS label FROM result"
    # pos = context.sql(query_6_pos)
    # query_6_neg = "SELECT *, IF(labeldjt = -1, 1, 0) AS label FROM result"
    # neg = context.sql(query_6_neg)
    #
    # pos.show()
    # neg.show()
    #
    #
    #
    # # task 7
    #
    #
    # # Initialize two logistic regression models.
    # # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    # poslr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
    # neglr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
    #
    # # set threshold
    # poslr.setThreshold(0.2)
    # neglr.setThreshold(0.25)
    #
    # # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
    # posEvaluator = BinaryClassificationEvaluator()
    # negEvaluator = BinaryClassificationEvaluator()
    # # There are a few parameters associated with logistic regression. We do not know what they are a priori.
    # # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
    # # We will assume the parameter is 1.0. Grid search takes forever.
    # posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
    # negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
    # # We initialize a 5 fold cross-validation pipeline.
    # posCrossval = CrossValidator(
    #     estimator=poslr,
    #     evaluator=posEvaluator,
    #     estimatorParamMaps=posParamGrid,
    #     numFolds=5)
    # negCrossval = CrossValidator(
    #     estimator=neglr,
    #     evaluator=negEvaluator,
    #     estimatorParamMaps=negParamGrid,
    #     numFolds=5)
    # # Although crossvalidation creates its own train/test sets for
    # # tuning, we still need a labeled test set, because it is not
    # # accessible from the crossvalidator (argh!)
    # # Split the data 50/50
    # posTrain, posTest = pos.randomSplit([0.5, 0.5])
    # negTrain, negTest = neg.randomSplit([0.5, 0.5])
    # # Train the models
    # print("Training positive classifier...")
    # posModel = posCrossval.fit(posTrain)
    # print("Training negative classifier...")
    # negModel = negCrossval.fit(negTrain)
    #
    # # Once we train the models, we don't want to do it again. We can save the models and load them again later.
    # posModel.save("project2/pos.model")
    # negModel.save("project2/neg.model")


    # task 8
    # timestamp: created_utc FROM comments
    # title:
    # state: author_flair_text FROM comments

    context.registerFunction("sub", subString, StringType())
    # context.registerFunction("process", preprocess, ArrayType(StringType()))

    submissions.createOrReplaceTempView("submissions")
    comments.createOrReplaceTempView("comments")

    query_8 = "SELECT submissions.id as id, comments.created_utc AS Timestamp, process(comments.body) AS comment, comments.author_flair_text AS State, submissions.title, submissions.score AS sub_score, comments.score as comm_score"+\
              " FROM comments JOIN submissions ON sub(comments.link_id)=submissions.id"\
              +" AND comments.body NOT LIKE '%/s%' and comments.body NOT LIKE '&gt%'"
    table8 = context.sql(query_8)
    # table8.show(20)
    print(table8.count())



    # task 9
    # model9 = cv.fit(table8)
    # table8.createOrReplaceTempView("table8")
    result9 = model.transform(table8)
    # result9.show()

    posModel = CrossValidatorModel.load("project2/pos.model")
    negModel = CrossValidatorModel.load("project2/neg.model")

    # context.registerFunction("prob", getProbability)

    posResult = posModel.transform(result9)#.selectExpr("features", "id", "Timestamp", "title", "State", "comment", "sub_score", "comm_score", "probability.getItem(1) as probability", "prediction as pos_label", "rawPrediction as pos_raw")
    # posResult = posModel.transform(result9)
    # posResult = posResult.withColumn("probability0", posResult["probability"].getItem(0)).withColumn("probability1", posResult["probability"].getItem(1))
    negResult = negModel.transform(result9)#.selectExpr("features", "id", "Timestamp", "title", "State", "comment", "sub_score", "comm_score", "probability.getItem(1) as probability", "prediction as neg_label", "rawPrediction as neg_raw")
    # negResult = negModel.transform(result9)
    #
    posResult.show()
    negResult.show()



    ########### debug
    # posTime = posResult.selectExpr("FROM_UNIXTIME(Timestamp) AS date")
    # posTime.show()

    # task 10
    posResult.createOrReplaceTempView("posResult")
    # posPercent = context.sql("SELECT AVG(prediction) AS posPercent FROM posResult")
    # posPercent.show()
    # +------------------+
    # | avg(prediction) |
    # +------------------+
    # | 0.3956210557855508 |
    # +------------------+
    negResult.createOrReplaceTempView("negResult")
    # negPercent = context.sql("SELECT AVG(prediction) AS negPercent FROM negResult")
    # negPercent.show()
    # +------------------+
    # | negPercent |
    # +------------------+
    # | 0.8984325273575168 |
    # +------------------+

    # by date
    # dayPosPercent = context.sql("SELECT FROM_UNIXTIME(Timestamp) AS date, AVG(prediction) AS dayPosPercent FROM posResult GROUP BY date")
    # dayNegPercent = context.sql("SELECT FROM_UNIXTIME(Timestamp) AS date, AVG(prediction) AS dayNegPercent FROM negResult GROUP BY date")
    # # dayPosPercent.show()
    # # dayNegPercent.show()
    # dayPosPercent.write.parquet("dayPosPercent")
    # dayNegPercent.write.parquet("dayNegPercent")
    #
    # reload date
    #
    # # dayPosPercent = context.read.parquet("dayPosPercent")
    # # dayNegPercent = context.read.parquet("dayNegPercent")
    # # dayPosPercent.show(20)
    # # dayNegPercent.show(20)
    # dayPosPercent.write.csv('dayPosPercent.csv')
    # dayNegPercent.write.csv('dayNegPercent.csv')

    # by states
    # context.registerFunction("stateCheck", check_state, BooleanType())
    # statePosPercent = context.sql("SELECT State, AVG(prediction) AS statePosPercent FROM posResult WHERE stateCheck(State) GROUP BY State")
    # stateNegPercent = context.sql("SELECT State, AVG(prediction) AS stateNegPercent FROM negResult WHERE stateCheck(State) GROUP BY State")
    # statePosPercent.show()
    # stateNegPercent.show()
    # statePosPercent.write.parquet("statePosPercent")
    # stateNegPercent.write.parquet("stateNegPercent")
    #
    # reload states
    #
    # statePosPercent = context.read.parquet("statePosPercent")
    # stateNegPercent = context.read.parquet("stateNegPercent")

    # by comment_score
    comPosPercent = context.sql("SELECT comm_score, AVG(prediction) AS comPosPercent FROM posResult GROUP BY comm_score")
    comNegPercent = context.sql("SELECT comm_score, AVG(prediction) AS comNegPercent FROM negResult GROUP BY comm_score")
    comPosPercent.show()
    comNegPercent.show()
    comPosPercent.write.parquet("comPosPercent")
    comNegPercent.write.parquet("comNegPercent")

    # by sub_score
    subPosPercent = context.sql("SELECT sub_score, AVG(prediction) AS subPosPercent FROM posResult GROUP BY sub_score")
    subNegPercent = context.sql("SELECT sub_score, AVG(prediction) AS subNegPercent FROM negResult GROUP BY sub_score")
    subPosPercent.show()
    subNegPercent.show()
    subPosPercent.write.parquet("subPosPercent")
    subNegPercent.write.parquet("subNegPercent")



if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)


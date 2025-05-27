// Import necessary libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

// Initialize Spark session
val spark = SparkSession.builder().appName("TelcoChurnPrediction").getOrCreate()

// Load data from Parquet
val analyzedData = spark.read.parquet("hdfs:/// analyzed_telco_reviews")

// Create ChurnRisk label
val churnRisk = analyzedData.withColumn("ChurnRisk", when(col("Rating") <= 2 && col("SentimentScore") < 0, 1.0).otherwise(0.0))

// Feature engineering - StringIndexer
val telcoIndex = new StringIndexer().setInputCol("Telco").setOutputCol("TelcoIndex")

// Feature engineering - VectorAssembler
val assembler = new VectorAssembler().setInputCols(Array("ReviewLength", "ResponseTime", "TelcoIndex")).setOutputCol("Features")

// Random Forest model - FIXED: setFeaturesCol to "Features" (matches VectorAssembler output)
val randomForest = new RandomForestClassifier() .setLabelCol("ChurnRisk") .setFeaturesCol("Features") .setProbabilityCol("RawProbability") .setPredictionCol("Prediction") .setNumTrees(50)

// Pipeline
val pipeline = new Pipeline().setStages(Array(telcoIndex, assembler, selector, randomForest))

// Cast columns to numeric types 
val dataWithNumericFeatures = churnRisk .withColumn("ReviewLength", col("ReviewLength").cast("double")) .withColumn("ResponseTime", col("ResponseTime").cast("double")) 

// Split data 
val Array(train, test) = dataWithNumericFeatures.randomSplit(Array(0.8, 0.2), seed = 42)

// Train model
val model = pipeline.fit(train)

// Make predictions
val predictions = model.transform(test)

// UDF to extract churn probability
val churnProb = udf((probability: Vector) => probability(1))

// Add ChurnProbability column
val churnPredictions = predictions.withColumn("ChurnProbability", churnProb(col("RawProbability")))

// Show results
churnPredictions.select("UserId", "TelcoIndex", "SentimentScore", "ChurnRisk", "Features", "RawProbability", "Prediction", "ChurnProbability").show()

// Save predictions to CSV
churnPredictions. select("UserId", "Telco", "Username", "Date", "Time", "Day", "Review", "Rating", "ReviewCategory", "ReviewLength", "ReviewLanguage", "ReviewResponse", "ResponseTime", "ThumbsUpCount", "UserState", "UserCountry"
, "Sentiment", "SentimentScore", "ChurnRisk", "ChurnProbability").write.option("header", "true") .mode("overwrite") .csv("hdfs:///output/predicted_telco_reviews.csv")

// Save predictions to Parquet
churnPredictions.write.mode("overwrite").parquet("hdfs:///output/ predicted_telco_reviews.parquet")

model.write.overwrite().save("hdfs:///output/churn_model")

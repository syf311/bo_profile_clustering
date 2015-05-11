package org.template.clustering

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext

import grizzled.slf4j.Logger

case class AlgorithmParams(
  numClusters: Int,
  numIterations: Int
) extends Params

// extends P2LAlgorithm because the MLlib's NaiveBayesModel doesn't contain RDD.
class KMeansAlgorithm(val ap: AlgorithmParams)
  extends P2LAlgorithm[PreparedData, KMeansModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): KMeansModel = {
    // MLLib NaiveBayes cannot handle empty training data.
    require(data.points.take(1).nonEmpty,
      s"RDD[points] in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preparator generates PreparedData correctly.")

    KMeans.train(data.points, ap.numClusters, ap.numIterations)
  }

  def predict(model: KMeansModel, query: Query): PredictedResult = {
    val cluster = model.predict(Vectors.dense(query.features))
    new PredictedResult(cluster)
  }
}

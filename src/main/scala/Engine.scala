package org.template.clustering

import io.prediction.controller.EngineFactory
import io.prediction.controller.Engine

class Query(
  val features: Array[Double]
) extends Serializable

class PredictedResult(
  val cluter: Int
) extends Serializable

class ActualResult(
  val cluster: Int
) extends Serializable

object ClusteringEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("kmeans" -> classOf[KMeansAlgorithm]),
      classOf[Serving])
  }
}

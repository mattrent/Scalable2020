package utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

object Algorithms {

  def labelPropagation(sc: SparkContext, graph: Graph[String, Int]): Graph[VertexId, Int] = {
    //initialize the LPA graph by setting the label of each vertex to its identifier
    val lpaGraph = graph.mapVertices { (vid, _) => vid}

    lpaGraph
  }
}

package utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeTriplet, Edge, Graph, VertexId}

object Algorithms {
	def SNN(graph: Graph[String, Int]): Graph[String, Int] = {
		var adj = scala.collection.mutable.Map.empty[Int, Set[Int]]

		graph.edges.foreach((e:Edge[Int]) => {
			
		})

		val graphSNN = graph

		graphSNN
	}

  def labelPropagation(sc: SparkContext, graph: Graph[String, Int]): Graph[VertexId, Int] = {
    //initialize the LPA graph by setting the label of each vertex to its identifier
    val lpaGraph = graph.mapVertices { (vid, _) => vid}

    lpaGraph
  }
}

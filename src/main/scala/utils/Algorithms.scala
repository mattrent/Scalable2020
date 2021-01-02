package utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, VertexId}


object Algorithms {
	def SNN(graph: Graph[String, Int]): Graph[String, Int] = {
		val neighbors = graph.collectNeighborIds(EdgeDirection.Either).collectAsMap().par
		val graphSNN = Graph(graph.vertices,
			graph.edges.mapValues(
				e => neighbors(e.srcId)
				  .intersect(neighbors(e.dstId))
				  .length
			)
		)

		graphSNN
	}

  def labelPropagation(sc: SparkContext, graph: Graph[String, Int]): Graph[VertexId, Int] = {
    //initialize the LPA graph by setting the label of each vertex to its identifier
    val lpaGraph = graph.mapVertices { (vid, _) => vid}

    lpaGraph
  }
}

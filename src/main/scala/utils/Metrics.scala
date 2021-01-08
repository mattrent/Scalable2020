package utils

import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.parallel.ParMap

object Metrics {

	def density(graphLPA: Graph[VertexId, Int]): RDD[(VertexId, Float)] = {
		val community = graphLPA.vertices.groupBy(_._2).map(group => (group._1, group._2.map(pair => pair._1)))
		val neighbors = graphLPA.collectNeighborIds(EdgeDirection.Out).collectAsMap().par

		val internalDensity = community.map(c => (c._1,
		  if (possibleCommunityEdges(c) == 0) 0F
		  else (2 * internalCommunityEdges(c, neighbors)) / possibleCommunityEdges(c))
		)

		internalDensity
	}

	private def possibleCommunityEdges(community: (VertexId, Iterable[VertexId])): Float = {
		community._2.size * (community._2.size - 1).toFloat
	}

	private def internalCommunityEdges(community: (VertexId, Iterable[VertexId]), neighbors: ParMap[VertexId, Array[VertexId]]): Float = {
		community._2.map(id => (neighbors(id).intersect(community._2.toSeq.par)).size).sum.toFloat
	}
}

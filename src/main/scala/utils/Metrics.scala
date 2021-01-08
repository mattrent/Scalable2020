package utils

import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.parallel.ParMap

object Metrics {

	def density(graphLPA: Graph[VertexId, Int]): RDD[(VertexId, Double)] = {
		val community = graphLPA.vertices.groupBy(_._2).map(group => (group._1, group._2.map(pair => pair._1)))
		val neighbors = graphLPA.collectNeighborIds(EdgeDirection.Out).collectAsMap().par

		val internalDensity = community.map(c => (c._1,
		  if (possibleCommunityEdges(c) == 0) 0D
		  else (2 * internalCommunityEdges(c, neighbors)) / possibleCommunityEdges(c))
		)

		internalDensity
	}

	private def possibleCommunityEdges(community: (VertexId, Iterable[VertexId])): Double = {
		community._2.size * (community._2.size - 1).toDouble
	}

	private def internalCommunityEdges(community: (VertexId, Iterable[VertexId]), neighbors: ParMap[VertexId, Array[VertexId]]): Double = {
		community._2.map(id => (neighbors(id).intersect(community._2.toSeq.par)).size).sum.toDouble
	}

	private def totalCommunityOutEdges(community: (VertexId, Iterable[VertexId]), neighbors: ParMap[VertexId, Array[VertexId]]): Double = {
		community._2.map(id => (neighbors(id).size)).sum.toDouble
	}

	def separability(graphLPA: Graph[VertexId, Int]): RDD[(VertexId, Double)] = {
		val community = graphLPA.vertices.groupBy(_._2).map(group => (group._1, group._2.map(pair => pair._1)))
		val neighborsOut = graphLPA.collectNeighborIds(EdgeDirection.Out).collectAsMap().par
		val neighborsIn = graphLPA.collectNeighborIds(EdgeDirection.In).collectAsMap().par

		val internalSeparability = community.map(c => (c._1,{
			val ein=internalCommunityEdges(c,neighborsIn)
			val eout= totalCommunityOutEdges(c, neighborsOut)-ein
			if (eout == 0) 0D
			else ein/eout
		}))

		internalSeparability
	}

	def modularity(graphLPA: Graph[VertexId, Int]): Double = {
		val neighborsOut = graphLPA.collectNeighborIds(EdgeDirection.Out).collectAsMap().par
		val inDegrees = graphLPA.inDegrees.collectAsMap().par
		val outDegrees = graphLPA.outDegrees.collectAsMap().par
		val indexedNodes = graphLPA.vertices.zipWithIndex
		val nodeCombinations = indexedNodes.cartesian(indexedNodes)
		  .filter{
			case(a, b) => a._2 < b._2
		  }
		  .map{
			case(a, b) => (a._1, b._1)
		  }
		val m = graphLPA.edges.count().toDouble

		val graphModularity = nodeCombinations.map({
			case (node_i: (VertexId, VertexId), node_j: (VertexId, VertexId)) => {
				if (node_i._2 == node_j._2) {
					val A_ij =
						if (neighborsOut(node_i._1).contains(node_j._1)) 1D
						else 0D
					val d_i_out = outDegrees.getOrElse(node_i._1, 0).toDouble
					val d_j_in = inDegrees.getOrElse(node_j._1, 0).toDouble
					A_ij - (d_i_out * d_j_in / m)
				} else 0D
			}
		}).sum / m

		graphModularity
	}


}

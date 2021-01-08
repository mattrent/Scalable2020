package utils

import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.parallel.ParMap

object Metrics {

	def density_old(graphLPA: Graph[VertexId, Int],graph: Graph[String, Int]): ParMap[VertexId, Int] = {
		val community=graphLPA.vertices.groupBy(_._2).map(group => (group._1, group._2.map(pair => pair._1))).collectAsMap().par
		//print(community.foreach(println))
		val denominatore=graphLPA.vertices.groupBy(_._2).map(group => (group._1,group._2.size*(group._2.size-1)))
		//print(denominatore.lookup(30360).mkString(",").toInt)
		//denominatore.foreach(println)

		//val neighbors = graph.collectNeighbors(EdgeDirection.Out).collectAsMap()

		val neighbors=graph.collectNeighborIds(EdgeDirection.Out).collectAsMap().par
		//print(neighbors(18778).toSet)

		val numberEdgesInCommmunity=community.map(
			c => (c._1, c._2.map(id => (neighbors(id).toSet.intersect(c._2.toSet)).size).sum)
		)

		//println(numberEdgesInCommmunity.foreach(println))

		val internalDensity= community.map(
			c => (c._1, if (denominatore.lookup(c._1).mkString(",").toInt ==0) 0
			else (2 * numberEdgesInCommmunity (c._1))/denominatore.lookup(c._1).mkString(",").toInt)
		)
		//println(internalDensity.foreach(println))

		internalDensity

	}

	def density(graphLPA: Graph[VertexId, Int]): RDD[(VertexId, Float)] = {
		val community = graphLPA.vertices.groupBy(_._2).map(group => (group._1, group._2.map(pair => pair._1)))
		//val denominatore = graphLPA.vertices.groupBy(_._2).map(group => (group._1, group._2.size * (group._2.size - 1)))
		val neighbors = graphLPA.collectNeighborIds(EdgeDirection.Out).collectAsMap().par



		def denominatore(community: (VertexId, Iterable[VertexId])): Int = {
			community._2.size * (community._2.size - 1)
		}

		def numberEdgesInCommmunity(community: (VertexId, Iterable[VertexId]), neighbors: ParMap[VertexId, Array[VertexId]]): Int = {
			community._2.map(id => (neighbors(id).intersect(community._2.toSeq.par)).size).sum
		}

		//val neighbors = graph.collectNeighbors(EdgeDirection.Out).collectAsMap()


		/*val numberEdgesInCommmunity = community.map(
			c => (c._1, c._2.map(id => (neighbors(id).intersect(c._2.toSeq.par)).size).sum)
		)*/

		//println(numberEdgesInCommmunity.foreach(println))

		/*val internalDensity = community.map(
			c => (c._1, if (denominatore.lookup(c._1).mkString(",").toInt ==0) 0
			else (2 * numberEdgesInCommmunity(c._1)) / denominatore.lookup(c._1).mkString(",").toInt)
		)*/

		val internalDensity = for (
			c <- community
		)
			yield (c._1,
			  if (denominatore(c) == 0) 0F
			  else (2 * numberEdgesInCommmunity(c, neighbors)) / denominatore(c)
			)

		internalDensity

	}
}

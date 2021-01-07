package utils

import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}

import scala.collection.parallel.ParMap

object Metrics {
/**
  def density(graphLPA: Graph[VertexId, Int],graph: Graph[String, Int]): ParMap[VertexId, Int] = {
    val community=graphLPA.vertices.groupBy(_._2).map(group => (group._1, group._2.map(pair => pair._1)))
    //print(community.foreach(println))
    val denominatore=graphLPA.vertices.groupBy(_._2).map(group => (group._1,group._2.size*(group._2.size-1)))
    //print(denominatore.lookup(30360).mkString(",").toInt)
    //denominatore.foreach(println)

    //val neighbors = graph.collectNeighbors(EdgeDirection.Out).collectAsMap()

    val neighbors=graph.collectNeighborIds(EdgeDirection.Out)
    //print(neighbors(18778).toSet)

    val numberEdgesInCommmunity=community.map(
      c => (c._1, c._2.map(id => (neighbors(id).toSet.intersect(c._2.toSet)).size).sum)
    )
    println(numberEdgesInCommmunity.foreach(println))

    val internalDensity= community.map(
      c => (c._1, if (denominatore.lookup(c._1).mkString(",").toInt ==0) 0
      else (2 * numberEdgesInCommmunity (c._1))/denominatore.lookup(c._1).mkString(",").toInt)
    )
    //println(internalDensity.foreach(println))

    internalDensity

  }*/
}

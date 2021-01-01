package utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

object GraphBuilder {
	def buildGraphFromFiles(sc: SparkContext, nodeFile: String, edgeFile: String, csv: Boolean): Graph[String, Int] = {
		val nodes = sc.textFile(nodeFile)
		  .mapPartitionsWithIndex(
			  (index, elem) => if (index == 0 && csv) elem.drop(1) else elem
		  )
		  .map(line => line.split(","))
		  .map(parts => (parts(0).toLong, parts(1)))

		val edges = sc.textFile(edgeFile)
		  .mapPartitionsWithIndex(
			  (index, elem) => if (index == 0 && csv) elem.drop(1) else elem
		  )
		  .map(line => line.split(","))
		  .map(parts => (parts(0).toLong, parts(1).toLong))

		/* create the graph; now every node is connected to a username, and nodes are connected according to edges; every edge has weight 1 */
		val graph = Graph.fromEdgeTuples(edges, "Default").joinVertices(nodes) {
			(id, _, name) => name
		}

		graph

		/*
			//this requires GraphFrames instead of GraphX (GraphX works on RDD, GraphFrames on DataFrame); we use GraphX now, might change later
			val nodesDataframe = spark.read.option("header", "true").csv("data/musae_git_target.csv")
			val edgesDataframe = spark.read.option("header", "true").csv("data/musae_git_edges.csv")
		*/
	}
}

import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {
	def main(args: Array[String]): Unit = {
		//TODO: set spark configuration (local option is just for testing)
		val spark = SparkSession
		  .builder
		  .appName("Scalable2020")
		  .config("spark.master", "local")
		  .getOrCreate()

		val sc = spark.sparkContext

		/* loading the files, skipping first line (csv header) */
		/* load text file => drop first line => split on commas => create the node structure (or the edge structure) by converting the ids to long */

		val nodes = sc.textFile("data/musae_git_target.csv")
		  .mapPartitionsWithIndex(
			  (index, elem) => if (index == 0) elem.drop(1) else elem
		  )
		  .map(line => line.split(","))
		  .map(parts => (parts(0).toLong, parts(1)))

		val edges = sc.textFile("data/musae_git_edges.csv")
		  .mapPartitionsWithIndex(
			(index, elem) => if (index == 0) elem.drop(1) else elem
		  )
		  .map(line => line.split(","))
		  .map(parts => (parts(0).toLong, parts(1).toLong))

		/* create the graph; now every node is connected to a username, and nodes are connected according to edges; every edge has weight 1 */
		var graph = Graph.fromEdgeTuples(edges, "Default")
		graph = graph.joinVertices(nodes) {
			(id, _, name) => name
		}

		graph.triplets.collect.foreach(println)

		/*
		//this requires GraphFrames instead of GraphX (GraphX works on RDD, GraphFrames on DataFrame); we use GraphX now, might change later
		val nodesDataframe = spark.read.option("header", "true").csv("data/musae_git_target.csv")
		val edgesDataframe = spark.read.option("header", "true").csv("data/musae_git_edges.csv")
		*/
	}

}

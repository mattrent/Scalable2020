import breeze.numerics.constants.e
import utils.GraphBuilder
import utils.Algorithms
import utils.Metrics
import org.graphstream.graph
import org.graphstream.graph.{Graph, IdAlreadyInUseException, implementations}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Edge, EdgeDirection, VertexId}
import org.apache.spark.rdd.RDD
import org.graphstream.graph.implementations.{AbstractEdge, MultiGraph, MultiNode, SingleGraph, SingleNode}

import java.io.{BufferedWriter, File, FileWriter}



object Main extends App {
	override def main(args: Array[String]): Unit = {
		//TODO: set spark configuration (local option is just for testing)
		val spark = SparkSession
		  .builder
		  .appName("Scalable2020")
		  .config("spark.master", "local[8]")
		  .getOrCreate()

		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		/* loading the files, skipping first line (csv header) */
		/* load text file => drop first line => split on commas => create the node structure (or the edge structure) by converting the ids to long */

		val graph = GraphBuilder.buildGraphFromFiles(sc, "data/musae_git_target.csv", "data/musae_git_edges.csv", true)

		//graph.triplets.collect.foreach(println)
		/*val graphLabProp = LabelPropagation.run(graph,5)
		graphLabProp.vertices.groupBy(_._2).foreach(group => println((group._1, group._2.size)))*/

		if (args.length > 0 && args(0) == "Trent") {
			//TODO: test LPA performance on simplified graph

			/*val graphSNN = Algorithms.SNN(graph, true)
			graphSNN.triplets.collect.foreach(println)
			GraphBuilder.export(graphSNN, "graphSNN.gexf")*/

			/*spark.time({
				val graphLPA = Algorithms.labelPropagationMR(graph, 30)
				graphLPA.vertices.collect
			})

			spark.time({
				val graphLPA_old = Algorithms.labelPropagationMR_old(graph, 30)
				graphLPA_old.vertices.collect
			})*/


			val graphLPA = Algorithms.labelPropagationPregel(graph, 5)
			spark.time(Metrics.density(graphLPA).collect)

			/*val file = new File("graphLPA_MR.txt")
			val bw = new BufferedWriter(new FileWriter(file))
			graphLPA.vertices.foreach(v => bw.write(v.toString()))*/

			/*val file_old = new File("graphLPA_MR_old.txt")
			val bw_old = new BufferedWriter(new FileWriter(file_old))
			graphLPA_old.vertices.foreach(v => bw_old.write(v.toString()))*/


			//lpaGraph.vertices.groupBy(_._2._1).mapValues(_.size).foreach(println)
		} else {
			/**val lpaGraph = Algorithms.labelPropagation(sc, graph,5);
			lpaGraph.vertices.collect.foreach(println)*/
			/**val lpaGraph = Algorithms.labelPropagationPregel(graph,5);
			lpaGraph.vertices.groupBy(_._2).foreach(group => println((group._1, group._2.size)))*/


			val lpaGraph = Algorithms.labelPropagationPregel(graph,5);
			Metrics.density(lpaGraph).filter(pair => pair._2!=0.0F).foreach(println)

			/**
			val gr = new SingleGraph("GitGraph");

			val vertices: RDD[(VertexId, String)] = sc.parallelize(List(
				(1L, "A"),
				(2L, "B"),
				(3L, "C"),
				(4L, "D"),
				(5L, "E"),
				(6L, "F"),
				(7L, "G")))

			val edges: RDD[Edge[String]] = sc.parallelize(List(
				Edge(1L, 2L, "1-2"),
				Edge(1L, 3L, "1-3"),
				Edge(2L, 4L, "2-4"),
				Edge(3L, 5L, "3-5"),
				Edge(3L, 6L, "3-6"),
				Edge(5L, 7L, "5-7"),
				Edge(6L, 7L, "6-7")))


			for ((id,_) <- vertices.collect()) {
				val node = gr.addNode(id.toString).asInstanceOf[SingleNode]
			}

			for (Edge(x,y,_) <- edges.collect()) {
				try {
					val edge = gr.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
				} catch {
					case ex: IdAlreadyInUseException => println(s"IdAlreadyInUseException: ("+ x.toString, y.toString+")")
				}
			}
			gr.display()*/
	/**
			for ((x,y,_) <- graph.edges.collect()) {
				val edge = gr.addEdge(x.toString ++ y.toString,
					x.toString, y.toString,
					true).
					asInstanceOf[AbstractEdge]
			}

		gr.display()
	*/

			/**gr.addNode("A" );
			gr.addNode("B" );
			gr.addNode("C" );
			gr.addEdge("AB", "A", "B");
			gr.addEdge("BC", "B", "C");
			gr.addEdge("CA", "C", "A");

			gr.display()*/

		}

	}

}

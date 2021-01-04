import utils.GraphBuilder
import utils.Algorithms

import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.sql.SparkSession

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
			/*val graphSNN = Algorithms.SNN(graph)
			graphSNN.triplets.collect.foreach(println)*/

			args(1) match {
				case "pregel" => {
					println("Using Pregel...")
					spark.time(
						Algorithms.labelPropagationPregel(graph, 30)
					)
				}
				case "mr" => {
					println("Using MR...")
					spark.time(
						Algorithms.labelPropagationMR(graph, 30)
					)
				}
				case "library" => {
					println("Using library LP...")
					spark.time(
						LabelPropagation.run(graph, 30)
					)
				}
			}
			//lpaGraph.vertices.groupBy(_._2._1).mapValues(_.size).foreach(println)
		} else {
			/**val lpaGraph = Algorithms.labelPropagation(sc, graph,5);
			lpaGraph.vertices.collect.foreach(println)*/
			val lpaGraph = Algorithms.labelPropagationPregel(graph,5);
			lpaGraph.vertices.groupBy(_._2).foreach(group => println((group._1, group._2.size)))
		}

	}

}

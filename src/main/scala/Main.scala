import org.apache.spark.graphx.lib.LabelPropagation
import utils.GraphBuilder
import utils.Algorithms
import utils.Metrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}

import java.io.FileWriter




object Main extends App {
	override def main(args: Array[String]): Unit = {
		//TODO: set spark configuration (local option is just for testing)
		val spark = SparkSession
		  .builder
		  .appName("Scalable2020")
		  .getOrCreate()
		val sc = spark.sparkContext

		var vertFile = ""
		var edgeFile = ""
		var algorithm = "LPA"
		var simplify = false
		var metrics = false
		var csv = true
		var steps = 10
		var comm = false
		var time = false
		var resultsFile = ""
		var csvList = List.fill(13)(" ")


		args.sliding(2,2).toList.collect {
			case Array("--vertices", vFile: String) => vertFile = vFile
			case Array("--edges", eFile: String) => edgeFile = eFile
			case Array("--csv", flag: String) => csv = flag.toBoolean
			case Array("--simplify", flag: String) => simplify = flag.toBoolean
			case Array("--metrics", flag: String) => metrics = flag.toBoolean
			case Array("--algorithm", algName: String) => algorithm = algName
			case Array("--steps", s: String) => steps = s.toInt
			case Array("--communities", flag: String) => comm = flag.toBoolean
			case Array("--time", flag: String) => time = flag.toBoolean
			case Array("--results", rFile: String) => resultsFile = rFile
		}

		assert(!(metrics && algorithm == "SLPA"), "Current metrics can't be calculated on overlapping communities")
		assert((steps > 0), "There needs to be a positive number of steps")

		println("Vertices file: " + vertFile)
		println("Edges file: " + edgeFile)
		println("Simplified graph with SNN: " + simplify)
		println("Steps: " + steps)
		println("Show metrics: " + metrics)
		println("Show number of communities: " + comm)

		csvList = csvList.updated(0, algorithm)
		csvList = csvList.updated(1, steps.toString)
		csvList = csvList.updated(2, simplify.toString)

		val graph =
			if (simplify) GraphBuilder.simplifyGraph(
				Algorithms.SNN(
					GraphBuilder.buildGraphFromFiles(sc, vertFile, edgeFile, true)
				)
			)
			else GraphBuilder.buildGraphFromFiles(sc, vertFile, edgeFile, true)

		val lpaGraph = algorithm match {
			case "LPA" => Algorithms.LPA_MR(graph, steps)
			case "DLPA" => Algorithms.DLPA(graph, steps)
			case "LPA_spark" => LabelPropagation.run(graph, steps)
			case "LPA_pregel" => Algorithms.labelPropagationPregel(graph, steps)
			case default => null
		}

		val slpaGraph =
			if (algorithm == "SLPA") Algorithms.SLPA(graph, steps)
			else null

		if (slpaGraph != null) {
			for (i <- 3 to 11) csvList = csvList.updated(i, "X")
		}

		if (metrics) {
			val separability = Metrics.separability(lpaGraph)
			val modularity = Metrics.modularity(lpaGraph)
			val density = Metrics.density(lpaGraph)

			println("Modularity: " + modularity)
			csvList = csvList.updated(11, modularity.toString)

			println("Separability statistics: ")
			val sepStats = Metrics.getStatistics(separability.values)
			sepStats.foreach(pair => println(pair._1 + ": " + pair._2))
			csvList = csvList.updated(3, sepStats("max").toString)
			csvList = csvList.updated(4, sepStats("min").toString)
			csvList = csvList.updated(5, sepStats("mean").toString)
			csvList = csvList.updated(6, sepStats("median").toString)

			println("Density statistics: ")
			val densStats = Metrics.getStatistics(density.values)
			densStats.foreach(pair => println(pair._1 + ": " + pair._2))
			csvList = csvList.updated(7, densStats("max").toString)
			csvList = csvList.updated(8, densStats("min").toString)
			csvList = csvList.updated(9, densStats("mean").toString)
			csvList = csvList.updated(10, densStats("median").toString)
		}

		if (comm) {
			val communityAmount = {
				if (slpaGraph == null) lpaGraph.vertices.groupBy(_._2).keys.count()
				else slpaGraph.vertices.groupBy(_._2).keys.reduce((l1, l2) => l1.union(l2)).size
			}
			println("Total communities: " + communityAmount)
			csvList = csvList.updated(12, communityAmount.toString)
		}

		if (time) {
			println("Execution time (algorithm + vertices collect): ")
			algorithm match {
				case "LPA" => spark.time(Algorithms.LPA_MR(graph, steps).vertices.collect())
				case "DLPA" => spark.time(Algorithms.DLPA(graph, steps).vertices.collect())
				case "LPA_spark" => spark.time(LabelPropagation.run(graph, steps).vertices.collect())
				case "LPA_pregel" => spark.time(Algorithms.labelPropagationPregel(graph, steps).vertices.collect())
				case "SLPA" => spark.time(Algorithms.SLPA(graph, steps).vertices.collect())
			}
		}

		if (resultsFile != "") {
			val fw = new FileWriter(resultsFile, true)
			fw.write(csvList.mkString(","))
			fw.write("\n")
			fw.close()
		}

	}

}

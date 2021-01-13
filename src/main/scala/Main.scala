import breeze.numerics.constants.e
import utils.GraphBuilder
import utils.Algorithms
import utils.Metrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}




object Main extends App {
	override def main(args: Array[String]): Unit = {
		//TODO: set spark configuration (local option is just for testing)
		val spark = SparkSession
		  .builder
		  .appName("Scalable2020")
		  .getOrCreate()

		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		/* loading the files, skipping first line (csv header) */
		/* load text file => drop first line => split on commas => create the node structure (or the edge structure) by converting the ids to long */

		val graph = GraphBuilder.buildGraphFromFiles(sc, args(0), args(1), true)

		//graph.triplets.collect.foreach(println)
		/*val graphLabProp = LabelPropagation.run(graph,5)
		graphLabProp.vertices.groupBy(_._2).foreach(group => println((group._1, group._2.size)))*/

		if (args.length > 2 && args(2) == "Trent") {
			//TODO: test LPA performance on simplified graph

			/*val graphSNN = Algorithms.SNN(graph, true)
			graphSNN.triplets.collect.foreach(println)
			GraphBuilder.export(graphSNN, "graphSNN.gexf")*/

			val graphLPA = Algorithms.labelPropagationPregel(graph, 5)
			/*val modularity = Metrics.modularity(graphLPA)
			println(modularity) */
			spark.time(Algorithms.labelPropagationMR(graph, 30).vertices.collect)

			/*spark.time({
				val graphLPA = Algorithms.labelPropagationMR(graph, 30)
				graphLPA.vertices.collect
			})

			spark.time({
				val graphLPA_old = Algorithms.labelPropagationMR_old(graph, 30)
				graphLPA_old.vertices.collect
			})*/


			/*val graphLPA = Algorithms.labelPropagationPregel(graph, 5)
			spark.time(Metrics.density(graphLPA).collect)*/

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

			println("Arrivata")
			//val lpaGraph = Algorithms.labelPropagationPregel(graph,5);
/**
			val separability=Metrics.density(lpaGraph)
			println("Post separability")
			val s=separability.map(_._2)
			//println(Metrics.getStatistics(s))




			//Mediana
			val ordered = s.sortBy(r=>r, ascending = false)
			println("Ordinato")
			val m = ordered.count()/2
			println("Punto medio "+m)
			if (m.isValidInt) println("Oggetto a indice m")
			else{println("media tra oggetto a indice m e m+1")}
*/


	/**

			//Creazione del grafo senza nodi isolati
			val graphWithoutIsoltedNode = GraphBuilder.simplifyGraph(graph)

			println("Numero archi vecchio grafo: "+graph.edges.count()+" --- Numero nodi vecchio grafo: "+graph.vertices.count())
			println("Numero archi nuovo grafo: "+graphWithoutIsoltedNode.edges.count()+" --- Numero nodi nuovo grafo: "+graphWithoutIsoltedNode.vertices.count())
			println("")
			println("Confronto tra LPA con pregel")
			println("Grafo con nodi isolati")
			val graphLPA1=Algorithms.labelPropagationPregel(graph,5)
			println("Numero community "+graphLPA1.vertices.groupBy(_._2).map(group => (group._1, group._2.map(pair => pair._1))).count())

			println("Grafo senza nodi isolati")
			val graphLPA2=Algorithms.labelPropagationPregel(graphWithoutIsoltedNode,5)
			println("Numero community "+graphLPA2.vertices.groupBy(_._2).map(group => (group._1, group._2.map(pair => pair._1))).count())
			*/

			/**
			println("")
			println("Confronto tra LPA map reduce")
			println("Grafo con nodi isolati")
			spark.time(
				Algorithms.labelPropagationMR(graph,5)
			)
			println("Grafo senza nodi isolati")
			spark.time(
				Algorithms.labelPropagationMR(graphWithoutIsoltedNode,5)
			)
			*/
		}

	}

}

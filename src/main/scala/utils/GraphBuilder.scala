package utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeDirection, Graph}

object GraphBuilder {
	def buildGraphFromFiles(sc: SparkContext, nodeFile: String, edgeFile: String, csv: Boolean): Graph[String, Int] = {
		/*
		loading the files, skipping first line (csv header)
		load text file => drop first line => split on commas => create the node structure (or the edge structure) by converting the ids to long
		*/
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
	}

	def export[VD, ED](graph: Graph[VD, ED], fileName: String) = {
		def toGexf[VD,ED](g:Graph[VD,ED]) =
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
			  "<gexf xmlns=\"http://www.gexf.net/1.3draft\" version=\"1.3\">\n" +
			  "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
			  "    <nodes>\n" +
							g.vertices.map(v => "      " +
							  "<node id=\"" + v._1 + "\" " +
							  "label=\"" + v._2 + "\" />\n").collect.mkString +
			  "    </nodes>\n" +
			  "    <edges>\n" +
							g.edges.map(e => "      " +
								"<edge source=\"" + e.srcId +
								"\" target=\"" + e.dstId + "\" weight=\"" + e.attr +
								"\" />\n").collect.mkString +
			  "    </edges>\n" +
			  "  </graph>\n" +
			  "</gexf>"

		val pw = new java.io.PrintWriter(fileName)
		pw.write(toGexf(graph))
		pw.close
	}

	/**
	 * Metodo che restituisce un grafo semplificato nel quale non ci sono nodi isolati (cioè nodi che non hanno nessun arco
	 * in uscita e nessun arco in entrata) e archi con attributo pari a 0
	 * @param graph grafo di partenza che vogliamo semplificare
	 * @return grafo semplificato
	 */
	def simplifyGraph (graph: Graph[String, Int]): Graph[String, Int] ={
		val neighborsOut = graph.collectNeighborIds(EdgeDirection.Out).collectAsMap().par
		val neighborsIn = graph.collectNeighborIds(EdgeDirection.In).collectAsMap().par
		graph.subgraph((e=>e.attr!=0), ((vid,s) => neighborsIn(vid).isEmpty !=true && neighborsOut(vid).isEmpty !=true))
	}
}

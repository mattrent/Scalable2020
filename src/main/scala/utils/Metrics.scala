package utils

import org.apache.spark.SparkContext
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

	/**
	 * Metodo che calcola il numero di archi tra i nodi che appartengono alla community presa in considerazione
	 * @param community community del grafo considerata
	 * @param neighbors map calcolata con il metodo graphx collectNeighborIds (che considera gli archi in uscita o in entrata
	 *                  a seconda della necessità)
	 * @return numero di archi che collegano nodi che appartengono alla community considerata
	 */
	private def internalCommunityEdges(community: (VertexId, Iterable[VertexId]), neighbors: ParMap[VertexId, Array[VertexId]]): Double = {
		community._2.map(id => (neighbors(id).intersect(community._2.toSeq.par)).size).sum.toDouble
	}

	/**
	 * Metodo che restituisce il numero totale di archi uscenti da ogni ogni nodo della community
	 * @param community community del grafo considerata
	 * @param neighbors map che associa ad ogni vertice e la lista dei nodi del grafo che vengono raggiunti
	 *                  da un arco in uscita da tale vertice
	 * @return numero totale di archi uscenti da ogni ogni nodo della community
	 */
	private def totalCommunityOutEdges(community: (VertexId, Iterable[VertexId]), neighbors: ParMap[VertexId, Array[VertexId]]): Double = {
		community._2.map(id => (neighbors(id).size)).sum.toDouble
	}

	/**
	 * Metodo che calcola il il rapporto tra il numero di archi interni alla community e il
	 * numero di archi che puntano all'esterno di ogni community (chiamato grado di separability della community)
	 * @param graphLPA grafo su cui è stato eseguito un metodo di community detection
	 * @return RDD contenente coppie (nome Community, grado Separability)
	 */
	def separability(graphLPA: Graph[VertexId, Int]): RDD[(VertexId, Double)] = {
		//Calcolo di un RDD formato da coppie (<nome community>, <lista di nodi della community>)
		val community = graphLPA.vertices.groupBy(_._2).map(group => (group._1, group._2.map(pair => pair._1)))
		//Calcolo di una map che associa ad ogni vertice e la lista dei nodi del grafo che vengono raggiunti
		//da un arco in uscita da tale vertice
		val neighborsOut = graphLPA.collectNeighborIds(EdgeDirection.Out).collectAsMap().par
		//Calcolo di una map che associa ad ogni vertice e la lista dei nodi del grafo per cui esiste un arco che raggiunge
		//il nodo considerato
		val neighborsIn = graphLPA.collectNeighborIds(EdgeDirection.In).collectAsMap().par

		//Il grado di separability di una community misura il rapporto tra il numero di archi interni alla community e il
		// numero di archi che puntano all'esterno della community
		val internalSeparability = community.map(c => (c._1,{
			//ein contiene il numero di archi tra nodi della stessa community
			val ein=internalCommunityEdges(c,neighborsIn)
			//eout contiene il numero di archi che da un nodo interno alla community puntano ad un nodo esterno alla community
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

	/**
	 * Metodo che restituisce delle informazioni utili per confrontare tra loro i risultati delle metriche
	 * applicate all'output di un algoritmo di community detection
	 * @param metricOutput RDD di Double contenenti i valori restituiti dalla metrica per ogni community
	 * @return map che associa il nome di una statistica utile al suo valore
	 */
	def getStatistics ( metricOutput: RDD[ Double]): ParMap[String,Double] = {
		val m = metricOutput.count()
		val mean = metricOutput.mean()
		val ordered = metricOutput.sortBy(identity, ascending = false).zipWithIndex().map{
			case (value, index) => (index, value)
		}
		val min = ordered.lookup(m - 1).head
		val max = ordered.lookup(0).head
		val median =
			if (m % 2 == 0) (ordered.lookup(m/2 - 1).head + ordered.lookup(m/2).head) / 2
			else ordered.lookup((m + 1)/2 - 1).head
		ParMap(
			"max" -> max,
			"min" -> min,
			"mean" -> mean,
			"median" -> median
		)

	}


}

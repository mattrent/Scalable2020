package utils

import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId}

import scala.collection.parallel.{ParMap, ParSeq}
import scala.util.Random


object Algorithms {

	def SNN(graph: Graph[String, Int], simplify: Boolean = false): Graph[String, Int] = {
		val neighbors = graph.collectNeighborIds(EdgeDirection.Out).collectAsMap().par

		val graphSNN = Graph(graph.vertices,
			graph.edges.mapValues(
				e => neighbors(e.srcId)
				  .intersect(neighbors(e.dstId))
				  .length
			)
		)
		if (simplify) graphSNN.subgraph((e => e.attr != 0))
		else graphSNN
	}

	def labelPropagationMR(graph: Graph[String, Int], maxSteps: Int): Graph[(VertexId, String), Int] = {
		require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

		val lpaGraph = graph.mapVertices{ case (vid, name) => (vid, name) }
		val neighbors = graph.collectNeighborIds(EdgeDirection.Out).collectAsMap().par
		val vertices = lpaGraph.vertices.collectAsMap().par

		def propagate(g: Graph[(VertexId, String), Int],
					  steps: Int,
					  neighbors: ParMap[VertexId, Array[VertexId]],
					  v: ParMap[VertexId, (VertexId, String)]): Graph[(VertexId, String), Int]
		= {
			if (steps == 0) g
			else {
				val tempGraph = g.mapVertices {
					case (id, (label, name)) => {

						//per ogni nodo adiacente cerco nel grafo la corrente etichetta => successivamente la mappo al suo numero di occorrenze
						val labels: Map[VertexId, Int] = neighbors(id).map(adjId => {
							v(adjId)._1
						}).groupBy(identity).mapValues(_.size)

						//la nuova etichetta del nodo è quella col maggior numero di occorrenze (cerco il massimo su _._2, altrimenti troverebbe l'id più alto)
						val newLabel =
							if (labels.size > 0) {
								val maxRepetitions = labels.maxBy(_._2)._2
								val labelPool = labels.filter(l => l._2 == maxRepetitions).keys.toSeq.par
								val l = takeRandom(labelPool, new Random)
								v.updated(id, (l, name))
								l
							}
							else label

						(newLabel, name)
					}
				}
				propagate(tempGraph, steps - 1, neighbors, v)
			}
		}

		propagate(lpaGraph, maxSteps, neighbors, vertices)
	}

	def labelPropagationPregel(graph: Graph[String, Int], maxSteps: Int): Graph[VertexId, Int] = {
		require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

		//inizializzazione delle label di ogni nodo del grafo con una etichetta diversa per ogni nodo
		val lpaGraph = graph.mapVertices { case (vid, _) => vid }


		/**
		 * La funzione sendMessage del modello di computazione Pregel viene utilizzata da ciascun nodo
		 * per informare i suoi vicini della sua etichetta corrente. Per ogni tripletta, il nodo di origine riceverà
		 * l'etichetta del nodo di destinazione e viceversa
		 * @param e tripletta formata da ( (srcNodeName, srcLabelNode), (dstNodeName, dstLabelNode), attr )
		 * @return iteratore per muoverci nelle coppie contenenti le informazioni del nodo e i messaggi ricevuti da tale nodo
		 */
		def sendMessage(e: EdgeTriplet[VertexId, Int]): Iterator[(VertexId, Map[VertexId, Long])] = {
			Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
		}

		/**
		 * La funzione mergeMsg per combinare tutti i messaggi, ricevuti da un nodo dai suoi vicini in una singola Map.
		 * Se entrambi i messaggi contengono la stessa etichetta, sommiamo semplicemente il numero corrispondente di vicini
		 * per questa etichetta
		 * @param count1 prima map dei messaggi
		 * @param count2 seconda map dei messaggi
		 * @return map risultante contenente il merge dei messaggi ricevuti per ogni nodo
		 */
		def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long]): Map[VertexId, Long] = {
			//Per ogni chiave i della prima e della seconda map che contiene la label del nodo viene estratto il valore
			//della label dell prima map e il valore della label della seconda map e successivamente viene inserita nella
			// map risultante l'associazione della chiave i al valore di etichetta ottenuto dalla somma delle etichette delle
			//due label precedentemente estratte dalle due map in input
			(count1.keySet ++ count2.keySet).map { i =>
				val count1Val = count1.getOrElse(i, 0L)
				val count2Val = count2.getOrElse(i, 0L)
				i -> (count1Val + count2Val)
			}.toMap
		}

		/**
		 * La funzione vertexProgram viene usata dopo che un nodo ha ricevuto i messaggi dai suoi vicini per determinare la
		 * sua community label, essa viene scelta come la community label a cui appartiene attualmente la maggior parte
		 * dei suoi vicini.
		 * @param vid coppia (nodeName, nodeLabel) che rappresenta il nodo considerato
		 * @param attr attributo del nodo (nel nostro caso sempre 1
		 * @param message mappa dei messaggi ricevuti dal nodo
		 * @return
		 */
		def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
			if (message.isEmpty) attr else message.maxBy(_._2)._1
		}

		//La map iniziale dei messaggi è vuota
		val initialMessage = Map[VertexId, Long]()

		//Esecuzione del modello computazionale Pregel
		Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
			vprog = vertexProgram,
			sendMsg = sendMessage,
			mergeMsg = mergeMessage)

	}

	private def takeRandom[A](sequence: ParSeq[A], random: Random): A = {
		sequence(random.nextInt(sequence.length))
	}

	private def weighNodes(graph: Graph[String, Int], policy: String): Graph[(VertexId, String, Double), Int] = {
		policy match {
			case "none" => graph.mapVertices{ case (vid, name) => (vid, name, 1D) }
			case "inDegree" => {
				val inDegrees = graph.inDegrees.collectAsMap().par
				graph.mapVertices{ case (vid, name) => (vid, name, inDegrees(vid)) }
			}
		}
	}

}

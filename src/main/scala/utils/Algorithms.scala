package utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId}

import scala.collection.mutable


object Algorithms {
	def SNN(graph: Graph[String, Int]): Graph[String, Int] = {
		val neighbors = graph.collectNeighborIds(EdgeDirection.Either).collectAsMap().par
		val graphSNN = Graph(graph.vertices,
			graph.edges.mapValues(
				e => neighbors(e.srcId)
				  .intersect(neighbors(e.dstId))
				  .length
			)
		)

		graphSNN
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

	}

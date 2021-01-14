package utils

import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId}

import scala.collection.parallel.{ParMap, ParSeq}
import scala.util.Random


object Algorithms {

	def SNN(graph: Graph[String, Int], simplify: Boolean = false): Graph[String, Int] = {
		val neighbors = graph.collectNeighborIds(EdgeDirection.Either).collectAsMap().par

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
		val neighbors = graph.collectNeighborIds(EdgeDirection.Either).collectAsMap().par
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
			//Costruzione della map risultante dell'iterazione di scambio delle label tra i vertici
			(count1.keySet ++ count2.keySet).map { i =>
				//Numero di occorrenze della label i prima dell'iterazione
				val count1Val = count1.getOrElse(i, 0L)
				//Numero di occorrenze della label i durante lo scambio delle label
				val count2Val = count2.getOrElse(i, 0L)
				//Risultato: la label i viene associata alla somma delle occorrenze ricevute prima dello scambio e le occorrenze
				//ricevute durante lo scambio
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

	def DLPA(graph: Graph[String, Int], maxSteps: Int): Graph[(VertexId, String), Int] = {
		require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

		//inizializzazione delle label dei vertici del grafo (ad ognuno di essi viene associata una label diversa)
		val lpaGraph = graph.mapVertices{ case (vid, name) => (vid, name) }

		//Calcolo dei vicini che sono collegati al nodo considerato mediante un arco in uscita dallo stesso
		val neighborsOut = graph.collectNeighborIds(EdgeDirection.Out).collectAsMap().par
		//Calcolo dei vicini che sono collegati al nodo considerato mediante un arco in entrata nello stesso
		val neighborsIn = graph.collectNeighborIds(EdgeDirection.In).collectAsMap().par

		//Vertici del grafo come mappa
		val vertices = lpaGraph.vertices.collectAsMap().par

		//Calcolo degli in degree per ogni nodo
		val inDegrees = graph.inDegrees.collectAsMap().par
		//Calcolo degli out degree per ogni nodo
		val outDegrees = graph.outDegrees.collectAsMap().par
		//Calcolo del degree di ogni nodo
		val degrees= graph.degrees.collectAsMap().par

		def propagate(g: Graph[(VertexId, String), Int],
									steps: Int,
									neighborsIn: ParMap[VertexId, Array[VertexId]],
									neighborsOut: ParMap[VertexId, Array[VertexId]],
									v: ParMap[VertexId, (VertexId, String)]): Graph[(VertexId, String), Int]
		= {
			if (steps == 0) g
			else {
				val tempGraph = g.mapVertices {
					case (id, (label, name)) => {

						//Calcolo dei degree per le label dei nodi che sono associati al vertice considerato mediante un arco di input
						val labelsIn: Map[VertexId, Float] = neighborsIn(id).map(adjId => {
							val degree= 1-((inDegrees.getOrElse(id,0)*outDegrees.getOrElse(adjId,0)).toFloat/(degrees.getOrElse(adjId,0)*degrees.getOrElse(id,0)).toFloat)
							(v(adjId)._1, degree)
						}).groupBy(_._1).mapValues(pair => pair.map(_._2).sum)

						//Calcolo dei degree per le label dei nodi che sono associati al vertice considerato mediante un arco di output
						val labelsOut: Map[VertexId, Float] = neighborsOut(id).map(adjId => {
							val degree= 1-((outDegrees.getOrElse(id,0)*inDegrees.getOrElse(adjId,0)).toFloat/(degrees.getOrElse(adjId,0)*degrees.getOrElse(id,0)).toFloat)
							(v(adjId)._1, degree)
						}).groupBy(_._1).mapValues(pair => pair.map(_._2).sum)

						//Calcolo dei degree totali di ogni label mediante il merge tra le due map costruiti al punto precedente
						val labels: Map[VertexId, Float] = (labelsIn.keySet ++ labelsOut.keySet).map { l =>
							val countLIn= labelsIn.getOrElse(l,0.0F)
							val countLOut= labelsOut.getOrElse(l,0.0F)
							l -> (countLIn+countLOut)
						}.toMap

						//Selezione della nuova etichetta del nodo come label con degree maggiore
						val newLabel =
							if (labels.size > 0) {
								val maxRepetitions = labels.maxBy(_._2)._2
								val labelPool = labels.filter(l => l._2 == maxRepetitions).keys.toSeq.par
								val l = takeRandom(labelPool, new Random)
								vertices.updated(id, (l, name))
								l
							}
							else label

						(newLabel, name)
					}
				}
				propagate(tempGraph, steps - 1, neighborsIn, neighborsOut, v)
			}
		}

		propagate(lpaGraph, maxSteps,neighborsIn, neighborsOut, vertices)
	}

}

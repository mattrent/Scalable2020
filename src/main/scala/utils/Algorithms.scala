package utils

import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, Pregel, VertexId}
import org.apache.spark.sql.catalyst.expressions.ToDegrees

import scala.collection.mutable.ListBuffer
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

	/**
	 * Metodo di calcolo classico di LPA
	 * @param graph grafo su cui vogliamo identificare le community
	 * @param maxSteps numero di cicli di label propagation
	 * @return grafo suddiviso in community, per ogni nodo viene specificata la community di appartenenza
	 */
	def labelPropagationMR(graph: Graph[String, Int], maxSteps: Int): Graph[VertexId, Int] = {
		require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

		val lpaGraph = graph.mapVertices{ case (vid, _) => vid }
		val neighbors = graph.collectNeighborIds(EdgeDirection.Either).collectAsMap().par
		val vertices = lpaGraph.vertices.collectAsMap().par

		def propagate(g: Graph[VertexId, Int],
					  steps: Int,
					  neighbors: ParMap[VertexId, Array[VertexId]],
					  v: ParMap[VertexId, VertexId]): Graph[VertexId, Int]
		= {
			if (steps == 0) g
			else {
				val tempGraph = g.mapVertices {
					case (id, label) => {

						//per ogni nodo adiacente cerco nel grafo la corrente etichetta => successivamente la mappo al suo numero di occorrenze
						val labels: Map[VertexId, Int] = neighbors(id).map(adjId => {
							v(adjId)
						}).groupBy(identity).mapValues(_.size)

						//la nuova etichetta del nodo è quella col maggior numero di occorrenze (cerco il massimo su _._2, altrimenti troverebbe l'id più alto)
						val newLabel =
							if (labels.size > 0) {
								val maxRepetitions = labels.maxBy(_._2)._2
								val labelPool = labels.filter(l => l._2 == maxRepetitions).keys.toSeq.par
								val l = takeRandom(labelPool, new Random)
								v.updated(id, l)
								l
							}
							else label

						newLabel
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

	//METODO PER CALCOLO DEI PESI DELLE LABEL PER DLPA (SECONDO ME NON HA SENSO FARLO SEPARATO)
	/**
	def getWeightedLabels (labelType: String,
												 neighbors: ParMap[VertexId, Array[VertexId]],
												 inDegrees: ParMap[VertexId,Int],
												 outDegrees: ParMap[VertexId,Int],
												 degrees: ParMap[VertexId,Int],
												 v: ParMap[VertexId, VertexId], id: VertexId ): Map[VertexId, Float]={

		labelType match {
			case "in"=>{
				//Calcolo dei degree per le label dei nodi che sono associati al vertice considerato mediante un arco di input
				val labelsIn: Map[VertexId, Float] = neighbors(id).map(adjId => {
					val degree= 1-((inDegrees.getOrElse(id,0)*outDegrees.getOrElse(adjId,0)).toFloat/(degrees.getOrElse(adjId,0)*degrees.getOrElse(id,0)).toFloat)
					(v(adjId), degree)
				}).groupBy(_._1).mapValues(pair => pair.map(_._2).sum)
				labelsIn
			}
			case "out"=>{
				//Calcolo dei degree per le label dei nodi che sono associati al vertice considerato mediante un arco di output
				val labelsOut: Map[VertexId, Float] = neighbors(id).map(adjId => {
					val degree= 1-((outDegrees.getOrElse(id,0)*inDegrees.getOrElse(adjId,0)).toFloat/(degrees.getOrElse(adjId,0)*degrees.getOrElse(id,0)).toFloat)
					(v(adjId), degree)
				}).groupBy(_._1).mapValues(pair => pair.map(_._2).sum)
				labelsOut
			}
		}

	}
	*/

	/**
	 * Variante del metodo classico di label propagation per la community detection specifico per i grafi diretti, chiamato
	 * Directed Label Propagation Algorithm (DLPA).
	 * @param graph grafo su cui vogliamo identificare le community
	 * @param maxSteps numero di cicli di label propagation
	 * @return grafo suddiviso in community, per ogni nodo viene specificata la community di appartenenza
	 */
	def DLPA(graph: Graph[String, Int], maxSteps: Int): Graph[VertexId, Int] = {
		require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

		//inizializzazione delle label dei vertici del grafo (ad ognuno di essi viene associata una label diversa)
		val lpaGraph = graph.mapVertices{ case (vid, name) => vid }

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

		//Funzione di propagazione delle label da parte dei nodi del grafo
		def propagate(g: Graph[VertexId, Int],
									steps: Int,
									neighborsIn: ParMap[VertexId, Array[VertexId]],
									neighborsOut: ParMap[VertexId, Array[VertexId]],
									v: ParMap[VertexId, VertexId]): Graph[VertexId, Int]
		= {
			if (steps == 0) g
			else {
				val tempGraph = g.mapVertices {
					case (id, label) => {
						//Calcolo dei degree per le label dei nodi che sono associati al vertice considerato mediante un arco di input
						val labelsIn: Map[VertexId, Float] = neighborsIn(id).map(adjId => {
							val degree= 1-((inDegrees.getOrElse(id,0)*outDegrees.getOrElse(adjId,0)).toFloat/(degrees.getOrElse(adjId,0)*degrees.getOrElse(id,0)).toFloat)
							(v(adjId), degree)
						}).groupBy(_._1).mapValues(pair => pair.map(_._2).sum)

						//Calcolo dei degree per le label dei nodi che sono associati al vertice considerato mediante un arco di output
						val labelsOut: Map[VertexId, Float] = neighborsOut(id).map(adjId => {
							val degree= 1-((outDegrees.getOrElse(id,0)*inDegrees.getOrElse(adjId,0)).toFloat/(degrees.getOrElse(adjId,0)*degrees.getOrElse(id,0)).toFloat)
							(v(adjId), degree)
						}).groupBy(_._1).mapValues(pair => pair.map(_._2).sum)


						/**
            val labelsIn: Map[VertexId, Float] =getWeightedLabels("in",neighborsIn,inDegrees,outDegrees,degrees,v,id)

						val labelsOut: Map[VertexId, Float] =getWeightedLabels("out",neighborsOut,inDegrees,outDegrees,degrees,v,id)
						*/

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
								vertices.updated(id, l)
								l
							}
							else label

						newLabel
					}
				}
				propagate(tempGraph, steps - 1, neighborsIn, neighborsOut, v)
			}
		}

		propagate(lpaGraph, maxSteps,neighborsIn, neighborsOut, vertices)
	}


	/**
	 * Variante del metodo classico di label propagation per la overlapping community detection, chiamato
	 * Speaker-Listener Label Propagation Algortithm (SLPA).
	 * Ad ogni nodo possono essere associate uno o più community label, in questo modo si identificano community sovrapposte
	 * poichè saranno presenti dei nodi che appartengono a più community
	 * @param graph grafo su cui vogliamo identificare le community
	 * @param maxSteps numero di cicli di label propagation
	 * @return grafo suddiviso in community, per ogni nodo viene specificata la lista di community di appartenenza
	 */
	def SLPA(graph: Graph[String, Int], maxSteps: Int): Graph[ListBuffer[VertexId], Int] = {
		require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

		//inizializzazione delle label dei vertici del grafo (ad ognuno di essi viene associata una label diversa)
		val lpaGraph = graph.mapVertices{ case (vid, name) => ListBuffer(vid) }

		//Vertici del grafo come mappa
		val vertices = lpaGraph.vertices.collectAsMap().par

		//Vicini
		val neighbors = graph.collectNeighborIds(EdgeDirection.Either).collectAsMap().par

		//Funzione per le T=maxStep di propagazione delle label tra i nodi
		def propagate (g: Graph[ListBuffer[VertexId], Int],
									 steps: Int,
									 neighbors: ParMap[VertexId, Array[VertexId]],
									 v: ParMap[VertexId, ListBuffer[VertexId]]): Graph[ListBuffer[VertexId], Int]
		= {
			if (steps == 0) g
			else {
				val tempGraph: Graph[ListBuffer[VertexId], Int] = g.mapVertices {
					//(id, (label, name)) rappresenta il nodo selezionato come listener
					case (id, label) => {

						//Ogni vicino del nodo listebere invia a tale nodo una singola label scelta mediante la speaking rule:
						//selezione casuale di un'etichetta dalla memoria del nodo vicino considerato con probabilità proporzionale alla
						//frequenza di una determinata label
						val labels: Map[VertexId, Int] = neighbors(id).map(adjId => {
							//Calcolo delle label possibili
							val totalNeighbors = v(adjId).size.toDouble
							val possibleLabels = v(adjId).groupBy(identity).mapValues(_.size.toDouble).par
							takeWeightedRandom(possibleLabels, new Random)
						}).groupBy(identity).mapValues(_.size)

						//Il listener seleziona una label dalla lista ottenuta dai vicini secondo la listening rule:
						//Seleziona la label più popolare che ha ottenuto dai vicini e la aggiunge alla propria memoria
						val newLabel: ListBuffer[VertexId] =
							if (labels.size > 0) {
								//Calcolo del valore di occorrenze più alto
								val maxRepetitions = labels.maxBy(_._2)._2
								//Lista delle label con numero di occorrenze più alto
								val labelPool = labels.filter(l => l._2 == maxRepetitions).keys.toSeq.par
								//Scelta random tra le label con numero di occorrenze più alto
								val l = takeRandom(labelPool, new Random)
								//Aggiornamento delle label associate al nodo listener
								v.updated(id, label+=l)

								label+=l
							}
							else label

						newLabel
					}
				}
				propagate(tempGraph, steps - 1, neighbors, v)
			}
		}

		//Fase di label propagation
		val postPropagateGraph=propagate(lpaGraph, maxSteps, neighbors, vertices)

		//Fase di post processing
		val postProcessingGraph: Graph[ListBuffer[VertexId], Int] = postPropagateGraph.mapVertices {
			//(id, (label, name)) rappresenta il nodo selezionato
			case (id, label) => {
				//Calcolo del numero di occorrenze per ogni label
				val occurrencesLabel= label.groupBy(identity).mapValues(_.size)
				//Calcolo del numero di label
				val nLabel=occurrencesLabel.keySet.size
				//Definizione del threashold
				val threashold= 0.5
				//Eliminiamo i duplicati nella lista delle label del nodo
				val labelNoDuplicates= label.distinct
				//Calcolo della lista di label la cui probabilità supera la soglia
				val newLabelSet=labelNoDuplicates.filter(vid=>occurrencesLabel.getOrElse(vid, 0)/nLabel >= threashold)

				newLabelSet
			}
		}

		postProcessingGraph
	}

	//Metodo di scelta random all'interno di una sequenza di label
	private def takeRandom[A](sequence: ParSeq[A], random: Random): A = {
		sequence(random.nextInt(sequence.length))
	}

	//Metodo di scelta random pesata all'interno di una sequenza di label
	private def takeWeightedRandom[A](sequence: ParMap[A, Double], random: Random): A = {
		val r = random.nextDouble()
		val weights = sequence.values
		val total = weights.sum
		val probabilities = weights.map(_ / total)
		val elements = sequence.keys.toSeq
		val index = probabilities.toStream.scanLeft(r)(_ - _).takeWhile(_ >= 0).size - 1
		elements(index)
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

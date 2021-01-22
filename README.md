# Progetto Scalable and Cloud Programming: Community Detection
## Obiettivi
- Implementazione di diverse versioni del Label Propagation Algorithm per la Community Detection
- Implementazione di diverse metriche utili per la valutazione delle community individuate dai differenti algoritmi
- Confronto tra gli algoritmi implementati e l'implementazione di LPA presente in libreria

## Community Detection
L'obiettivo di un algoritmo di community detection è quello di trovare gruppi di nodi di interesse all'interno di una rete, chiamati community.
Tipologie di Community Detection:
- *Non-Overlapping Community Detection*: ad ogni nodo del grafo viene attribuita una sola label rappresentante la community di appartenenza.
Gli algoritmi di tale tipo implementati all'interno del progetto sono i seguenti:
    - LPA
    - LPA Shuffle
    - DLPA
    - LPA con Pregel
- *Overlapping Community Detection*: ad ogni nodo del grafo viene attribuita una lista di label rappresentante le diverse community di appartenenza. L'algoritmo implementato all'interno del codice di tale progetto è SLPA (Speaker Listener Label Propagation Algorithm).

## Modalità di esecuzione dei test in locale
Per eseguire i test relativi al progetto è necessario eseguire i seguenti passi:
1) Eseguire il comando `sbt package` all'interno della cartella del progetto, questo comando costruirà il file jar relativo al progetto all'interno della cartella `Scalable2020/target/scala-2.12` chiamato `Scalable2020.jar`.
2) Eseguire il comando: 
	`spark-submit --master local[*] Scalable2020/target/scala-2.12/Scalable2020.jar <argomenti>`
Gli argomenti da specificare sono descritti in modo dettagliato nelle successive sezioni.
### Algoritmi di Non-Overlapping Community Detection
Per eseguire il test su uno di questi algoritmi è necessario specificare i seguenti argomenti:
- `--vertices <path_file_nodi_grafo>`: per specificare il file dei nodi del dataset.
- `--edges <path_file_nodi_grafo>`: per specificare il file degli archi del dataset.
- `--csv <true|false>`: per richiedere o meno il salvataggio dei risultati su un file csv.
- `--simplify  <true|false>`: specificando `true` si richiede l'esecuzione dell'algoritmo sul grafo semplificato, altrimenti l'esecuzione avviene sul grafo non semplificato.
- `--metrics  <true|false>`: specificando `true` si richiede il calcolo delle seguenti metriche: Separability, Modularity, Density. Inoltre, per la separability e la density vengono calcolate le seguenti statistiche: Min, Max, Media, Mediana
- `--algorithm  <LPA|DLPA|LPA_spark|LPA_pregel|LPA_shuffle>`: per specificare l'algoritmo di Non-Overlapping Community Detection che si intende eseguire.
- `--steps <numero_step>`: per specificare il numero di step di Label Propagation.
- `--metrics  <true|false>`: specificando `true` si richiede il calcolo del numero di community individuate dall'algoritmo.
- `--time  <true|false>`: specificando `true` si richiede il calcolo del tempo necessario per eseguire l'algoritmo, tale tampo di calcolo viene misurato utilizzando`spark.time`.
- `--results  <path_file_output>`: per specificare il file di output.
### Analisi di SLPA
Essendo SLPA l'unico algoritmo di Overlapping Community Detection implementato all'interno di questo progetto si è effettuata solamente un'analisi delle sue prestazioni senza effettuare nessun confronto.
Per eseguire il test su SLPA è necessario specificare gli argomenti nel seguente modo:
- `--vertices <path_file_nodi_grafo>`: per specificare il file dei nodi del dataset.
- `--edges <path_file_nodi_grafo>`: per specificare il file degli archi del dataset.
- `--csv <true|false>`: per richiedere o meno il salvataggio dei risultati su un file csv.
- `--metrics  false`: perché le metriche elencate precedentemente non sono applicabili ai risultati degli algoritmi di Overlapping Community Detection.
-`--algorithm  SLPA`
- ` --steps <numero_step>`: per specificare il numero di step di Label Propagation, per SLPA il numero minimo di step è 20 e il numero massimo è 100.
- `--time  <true|false>`: specificando `true` si richiede il calcolo del tempo necessario per eseguire l'algoritmo, tale tampo di calcolo viene misurato utilizzando `spark.time`.
- `--results  <path\_file\_output>`: per specificare il file di output.
- `--r  <valore>`: soglia necessaria per la fase di post-processing dell'algoritmo, il valore di tale soglia è compreso tra [ 0.01, 0.1 ]
## Caricamento ed esecuzione su cloud

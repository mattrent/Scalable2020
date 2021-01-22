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

## Contenuto del repository
- Codice del progetto
- `test_run,py` script python per l'esecuzione automatizzata dei test del progetto
- `Presentazione.pdf`file di presentazione del progetto
- `Report risultati.pdf` file contenente l'analisi di tutti i risultati ottenuti e confronto tra i diversi algoritmi di Non-Overlapping Community Detection

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
- `--communities <true|false>`: specificando `true` si richiede il calcolo del numero di community individuate dall'algortmo.
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
- `--communities <true|false>`: specificando `true` si richiede il calcolo del numero di community individuate dall'algortmo.
- `--results  <path\_file\_output>`: per specificare il file di output.
- `--r  <valore>`: soglia necessaria per la fase di post-processing dell'algoritmo, il valore di tale soglia è compreso tra [ 0.01, 0.1 ]
## Caricamento ed esecuzione su cloud
1) Eseguire il comando `sbt assembly` all'interno della cartella del progetto, questo comando costruirà il file jar relativo al progetto con tutte le dipendenze all'interno della cartella `Scalable2020/target/scala-2.12` chiamato `Scalable2020-assembly-0.1.jar`.
2) Connettersi al sito AWS Educate ed effettuare l'accesso
3) Andare nella sezione AWS Account
4) Cliccare su AWS Educate Starter Account
5) Cliccare su AWS Console
6) Entrare nel servizio s3
7) Creare un bucket s3 nel seguente modo
- Cliccare su Crea bucket
- Inserire il nome del bucket e cliccare sul bottone Crea bucket per completare l'operazione
8) Fare click sul bucket appena creato ed eseguire i seguenti passaggi:
- Caricare una cartella `data` contentente i file relativi al dataset e attendere il caricamento completo del file (all'interno del progetto è stato utilizzato il seguente dataset: LINK, su s3 sono stati caricati i file: `musae_git_edges.csv`, `musae_git_target.csv`)
-Caricare il file jar `Scalable2020-assembly-0.1.jar` creato al punto 1 e attendere il caricamento completo del file
![ ](/img/s3_completato.png  "Schermata di caricamento su s3 completato") 
9) Tornare alla pagina relativa a AWS Console
10) Selezionare il servizio EMR
11) Cliccare su Crea cluster
- Inserire il nome del cluster
- Selezionare la versione `emr-6.2.0`
- Selezionare l'applicazione `Spark: Spark 3.0.1 on Hadoop 3.2.1 YARN with and Zeppelin 0.9.0-preview1`
- Cliccare su Crea cluster
![ ](/img/configurazione_cluster.png  "Configurazione del cluster ")
12) Cliccare sulla sezione Fasi del cluster appena creato
13) Cliccare su Aggiungi fase
-Selezionare Applicazione Spark come tipologia di fase
- Selezionare il jar contenuto nel bucket di s3 sul quale si è precedentemente caricata l'applicazione che si intende eseguire
- Nel campo argomenti specificare, come esempio sono stati utilizzati i seguenti argomenti: `--vertices s3://scalable2020/data/musae_git_target.csv --edges s3://scalable2020/data/musae_git_edges.csv --csv false --simplify false --metrics false --algorithm LPA --steps 20 --metrics false --time true --communities true`
![ ](/img/fase_cluster.png  "Configurazione della fase del cluster")
14) Una volta completata l'esecuzione sarà possibile vedere il risultato di output andando su s3 ed eseguendo le eseguenti operazioni:
- Cliccare sul bucket di log
- Cliccare su elasticmapreduce/
- Cliccare sulla cartella corrispondente all'identificativo del cluster 
- Cliccare su containers/
- Cliccare sulla cartella application relativa alla fase di cui si vuole vedere l'output
- Cliccare sulla prima cartella container
- Cliccare sul file stdout.gz
- Fare click su Operazioni sugli oggetti e cliccare su Apri
![ ](/img/output.png  "Contenuto file di output")
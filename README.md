# PPS-18-spark-external-internal-analysis
Spark è un famoso framework Scala per big-data e cluster computing. In questo progetto si studieranno aspetti linguistici di questo framework, e la sua organizzazione interna -- l'obiettivo è porre le basi per futuri framework per computazione distribuita che si ispirino a Spark.

# [Spark](https://spark.apache.org/docs/latest/quick-start.html)
[Intro](https://towardsdatascience.com/introduction-to-apache-spark-with-scala-ed31d8300fe4)

Perchè è così importante?
- Astrae la programmazione parallela, non sembra di lavorare su un cluster di computer.
Nello scenario migliore sembrerà di lavorare con un database (SQL), nel peggiore di lavorare su collections.

- Piattaforma unificata, tutto in un singolo framework

- Facile da usare, leggere e capire

## Spark shell
Un [primo approccio](https://bigdata-madesimple.com/learning-scala-spark-basics-using-spark-shell-in-local/) a Spark.
## [Dataset API](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset)
Operazioni con cui interrogare il Dataset. 

Un Dataset si può creare anche "on the fly" da un file di testo o appartenere a determinati formati (es. Hadoop HDFS) o trasformando altri Dataset.

Nelle interrogazioni che lo richiedono vengono passate funzioni, per cui possono entrare in gioco aspetti di PPS.

## Caching
Anche su dati distribuiti in molti nodi.

# [Internals](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-overview.html)

![Architettura Spark](https://spark.apache.org/images/spark-stack.png)
Completa alternativa al map-reduce di Hadoop, molto più performante.
Alla base dell'ecosistema di `Spark` troviamo `SparkCore`, il quale, a sua volta, è diviso in due parti:

- __Computer Engine__: fornisce funzioni basilari come gestione della memoria, scheduling dei task, recupero guasti, interagisce con il Cluster Manager (che non viene fornito da Spark).
- __Spark Core APIS__: consiste in due API:
    - strutturate: DataFrame e DataSets, ottimizzate per lavorare con dati strutturati
    - non strutturate: RDDs, variabili Accumulators e Broadcast

Sopra Spark Core troviamo principalmente i 4 moduli:
- Spark SQL
- Spark Streaming
- MLlib
- GraphX

Offrono API, DLS e algoritmi in più linguaggi. Dipendono direttamente dalla base, ovvero __Spark Core__.
## Architettura
Posso lanciare Spark in essenzialmente due __modalità__:
- interattiva
- submit di un job (in caso di produzione). 

Queste modalità ci aprono al concetto di `master-worker` (PCD) che nel "dialetto di Spark" è noto come `driver-executor`.
Essenzialmente posso usare Spark con o senza un vero cluster:
- __local mode__: uso in fase esplorativa per iniziare ad usare spark senza un effettivo cluster
- con cluster :
    - __client mode__: il driver è nel client, per cui è preferibile usarla in fase di debug
    - __cluster mode__: il driver è nel cluster, la uso in produzione

Quando eseguo Spark lo stato dei processi è visibile in una pagina web (Spark UI).

__Chi controlla il cluster? Come Spark ottiene le risorse per driver e executor?__
Il `cluster manager`, il quale non viene offerto da Apache Spark.
- __Apache YARN__ per Hadoop
- __Apache Mesos__ (general purpose)
- Kubernetes: Google, non per fasi di produzione
- StandAlone: facile e veloce, non per produzione

`Spark session`

### RDDs  e tasks
Poichè DataSet e DataFrame ereditano da RDDs, capendo questi ultimi possiamo capire come effettivamente viene distribuito il lavoro tra gli executors.
Quando viene letto un file è possibile specificare il numero di __partizioni di un RDD__ che (guarda caso) coincide con il corrispondente __numero di task__.
Ovviamente, numero executors e di task sono effettivamente correlati e quindi bisogna specificare il numero di conseguenza.

Si lavora in __stage__, uno stage rappresenta un periodo (una o più funzione chiamata sui dati) in cui non è necessario uno shuffle, ovvero non è necessario spostare i dati tra le partizioni. Se i dati devono muoversi (es. reduceByKey) bisogna ripartizionare i dati (_shuffle & sort_).
Con `collect` si ritorna dagli executor al driver.

Nota interessante, in Scala, DataFrame è così definito [`type DataFrame = Dataset[Row]`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.package@DataFrame=org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]), ciò 
significa che su un DataFrame posso chiamare tutti i metodi del Dataset.

## ExecutionModel
- Crea DAG di RDDs per rappresentare la computazione
- Crea piano di esecuzione logico del DAG:
    - pipeline il più possibile
    - divide in __stages__
- Schedula e esegue i tasks:
    - divide ogni stage in task
    - task = dati + computazione
    - esegue tutti i task di uno stage prima di andare avanti

## More on Shuffle
- è pull based e non push based
- scrive file intermedi nei dischi

Occhio al numero di partizioni.
Troppe poche: 
- poca concorrenza
- più suscettibile a "data skew" %TODO
- maggiore uso della memoria in operazioni che richiedono Shuffle (groupBy, sortByKey, reduceByKey, etc.)
Troppe
Numero considerevole, di solito tra 100 e 10000
- lower bound, almeno circa il doppio del numero di core di un cluster
- upper bound: assicurare che un task ci metta almeno 100ms

Problemi di memoria: lentezza e errori, magari perchè si fanno troppi shuffle.

(da vedere https://databricks.com/session/a-deeper-understanding-of-spark-internals da min. 22)
# Moduli
## [RDDs (Resilient Distributed Datasets)](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- Concetto di `SparkContext`, uno per JVM.
- Modalità di lancio in locale (es. local[4], su 4 core) o su cluster 
- Concetti principali:
  - `transformation`: map, sono lazy l'eventuale azione su di essa applicherà la trasformazione. Eventualmente è possibile rendere questi cambiamenti persistenti, altrimenti non lo sono.
  - `actions`: reduce
- Passare funzioni a Spark
- Problema closure (PCD), soluzioni:
  - `broadcasts`
  - `accumulators` -> vale sempre il concetto lazy (vedi trasformazioni)
- printing: collect può fare 'out of memory', concetti simili agli stream (take(n)).
- shuffle nei nodi
- caching oltre persistent ci sono anche modalità per specificare a quale memoria attaccarsi.

## [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html)
Dati strutturati, maggiori ottimizzazioni rispetto a RDD. Interagisco in vari modi, tra cui SQL e Dataset API.
Nella computazione viene usato un unico engine di esecuzione, il programmatore può esprire le cose nel modo che ritiene più naturale. Uso di Hive, JDBC, etc. 
Non credo questo sia interessante sul lato PPS.

## [Structured Streaming over Spark SQL](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
## [Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
Dati che provengono da varie fonti (es. Kafka, Flume, Kinesis o TCP sockets) attravestro questo modulo possono essere processati usando algoritmi complessi espressi attravervo funzioni high-level come `map`, `reduce`, `join` and `window`.
In uscita si avranno File Systems (HDFS), Databases o Dashboard da eventualemente elaborare con le funzioni di spark ML e Graph.

Internamente Spark Streaming divide lo stream in batch di dati che vengono processati dalla Spark Engine.

La lettura di uno streaming eredita dall'astrazione `DStream`, si tratta di dati che provengono da varie fonti (es. Kafka, Flume, and Kinesis) o da operazione ad alto livello su altri DStream.
Un DStream può essere considerato come una sequenza di RDDs.
Le __operazioni su un DStream__, molto simili a quelle su un RDD, funzioni proprie si possono applicare 
solo attraverso quelle fornite dall'API, non dovrebbero esserci limitazioni.
DStream estende Serializable.

## [Machine Learning Library (MLlib)](https://spark.apache.org/docs/latest/ml-guide.html)
- Algoritmi di Machine Learning: come classificazione, regressione, clustering e collaborative filtering.
- Featurization: feature extraction, transformation, dimensionality reduction, and selection
- Pipelines: tools for constructing, evaluating, and tuning ML Pipelines
- Persistenza: saving and load algorithms, models, and Pipelines
- Utility: linear algebra, statistics, data handling, etc.
## [GraphX Programming](https://spark.apache.org/docs/latest/graphx-programming-guide.html#graphx-programming-guide)
Componente Spark per Grafi e calcolo graph-parallel che si trova sopra SparkCore.

![Passaggio SparkCore/GraphX](https://spark.apache.org/docs/1.2.1/img/graph_analytics_pipeline.png)

Ad alto livello viene estratto il concetto di RDD introducendo l'estensione Graph.
Ci sono varie operazioni a supporto di questo concetto, anche algoritmi e builder per l'analisi.

Quindi userei questa estensione quando nella natura dei grafi ci sono legami che vengono meglio rappresentati da un grafo,
prima di dare in pasto i dati a GraphX potrebbero servire minime operazioni per renderli "compatibili" con la visione a grafo.
Nella costruzione vengono richiesti:
- RDD dei vertici
- RDD degli edge
- un vertice di default (pozzo).
## [SparkR (R on Spark)](https://spark.apache.org/docs/latest/sparkr.html)
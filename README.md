# PPS-18-spark-external-internal-analysis
Spark è un famoso framework Scala per big-data e cluster computing. In questo progetto si studieranno aspetti linguistici di questo framework, e la sua organizzazione interna -- l'obiettivo è porre le basi per futuri framework per computazione distribuita che si ispirino a Spark.

# [Spark](https://spark.apache.org/docs/latest/quick-start.html)
[Intro](https://towardsdatascience.com/introduction-to-apache-spark-with-scala-ed31d8300fe4)
## Spark shell
Un [primo approccio](https://bigdata-madesimple.com/learning-scala-spark-basics-using-spark-shell-in-local/) a Spark.
## [Dataset API](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset)
Operazioni con cui interrogare il Dataset. 

Un Dataset si può creare anche "on the fly" da un file di testo o appartenere a determinati formati (es. Hadoop HDFS) o trasformando altri Dataset.

Nelle interrogazioni che lo richiedono vengono passate funzioni, per cui possono entrare in gioco aspetti di PPS.

## Caching
Anche su dati distribuiti in molti nodi.

# Moduli
## [RDDs (Resilient Distributed Datasets)](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- Concetto di `SparkContext`, una per JVM.
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

Internamente Spark Streaming diviede lo stream in batch di dati che vengono processati dalla Spark Engine.

Esiste un'astrazione di Spark che si chiama `DStream`, si tratta di dati che provengono da varie fonti (es. Kafka, Flume, and Kinesis) o da operazione ad alto livello su altri DStream.
Un DStream può essere considerato come una sequenza di RDDs.

## [Machine Learning Library (MLlib)](https://spark.apache.org/docs/latest/ml-guide.html)

## [GraphX Programming](https://spark.apache.org/docs/latest/graphx-programming-guide.html#graphx-programming-guide)

## [SparkR (R on Spark)](https://spark.apache.org/docs/latest/sparkr.html)

# [Internals](https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-overview.html)
#

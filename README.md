# PPS-18-spark-external-internal-analysis
Spark è un famoso framework Scala per big-data e cluster computing. In questo progetto si studieranno aspetti linguistici di questo framework, e la sua organizzazione interna -- l'obiettivo è porre le basi per futuri framework per computazione distribuita che si ispirino a Spark.

Il progetto analizza __Spark 2.4.4__ in *Scala*.
##Struttura del progetto
La relazione finale del progetto è nel file [`report.pdf`](report.pdf).

All'interno delle risorse del progetto ci sono:

- analisi delle funzionalità base di __Spark Core__: [`/src/main/scala/QuickStartBasic.scala`](/src/main/scala/QuickStartBasic.scala)
che si basa sul rispettivo container _docker_ presente in
[`/src/main/docker/basic-container`](/src/main/docker/basic-container)

- analisi delle funzionalità base di __SparkSQL__: [`/src/main/scala/QuickStartSparkSQL.scala`](/src/main/scala/QuickStartSparkSQL.scala) 
che si basa sul rispettivo container _docker_ presente in
[`/src/main/docker/basic-container`](/src/main/docker/basic-container)

- analisi dello streaming:

    * analisi delle funzionalità di __Spark Streaming__: 
    [`/src/main/scala/streaming/SparkStreaming.scala`](/src/main/scala/streaming/SparkStreaming.scala) che si basa sul rispettivo
    container _docker_ presente in [`/src/main/docker/stream-container`](/src/main/docker/stream-container)
   
    * analisi delle funzionalità di __Spark Structured Streaming__: 
    [`/src/main/scala/streaming/structuredStreaming/SparkStructuredStreaming.scala`](/src/main/scala/streaming/structuredStreaming/SparkStructuredStreaming.scala) che si basa sul rispettivo
     container _docker_ presente in [`/src/main/docker/stream-container`](/src/main/docker/stream-container)
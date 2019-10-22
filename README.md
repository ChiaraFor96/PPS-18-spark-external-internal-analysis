# PPS-18-spark-external-internal-analysis
Spark è un famoso framework Scala per big-data e cluster computing. In questo progetto si studieranno aspetti linguistici di questo framework, e la sua organizzazione interna -- l'obiettivo è porre le basi per futuri framework per computazione distribuita che si ispirino a Spark.

Il progetto analizza __Spark 2.4.4__ in *Scala*.

## Struttura del progetto
La relazione finale del progetto è nel file [`report.pdf`](report.pdf).

All'interno delle risorse del progetto, ci sono esempi che vanno a completare
l'analisi svolta.

### Analisi delle funzionalità base di __Spark Core__
Viene svolta in [`/src/main/scala/QuickStartBasic.scala`](/src/main/scala/QuickStartBasic.scala)
e si basa sul rispettivo container _docker_ presente in
[`/src/main/docker/basic-container`](/src/main/docker/basic-container).
__Istruzioni di lancio__:
- aggiungere la riga `127.0.0.1   localhost   hadoop` in `etc/hosts
- spostarsi da terminale nella cartella [`/src/main/docker/basic-container`](/src/main/docker/basic-container)
- lanciare i comandi:
    - `docker build -t container-name .`
    - `docker run -ti --hostname hadoop -p 50070:50070 -p 9000:9000 -p 50075:50075 -p 50010:50010 container-name`
- lanciare il main di [`/src/main/scala/QuickStartBasic.scala`](/src/main/scala/QuickStartBasic.scala)


### Analisi delle funzionalità base di __SparkSQL__
Viene svolta in [`/src/main/scala/QuickStartSparkSQL.scala`](/src/main/scala/QuickStartSparkSQL.scala) 
e si basa sul rispettivo container _docker_ presente in
[`/src/main/docker/basic-container`](/src/main/docker/basic-container).
__Istruzioni di lancio__:
- aggiungere la riga `127.0.0.1   localhost   hadoop` in `etc/hosts`
- spostarsi da terminale nella cartella [`/src/main/docker/basic-container`](/src/main/docker/basic-container)
- lanciare i comandi:
    - `docker build -t container-name .`
    - `docker run -ti --hostname hadoop -p 50070:50070 -p 9000:9000 -p 50075:50075 -p 50010:50010 container-name`
- lanciare il main di [`/src/main/scala/QuickStartSparkSQL.scala`](/src/main/scala/QuickStartSparkSQL.scala) 

### Analisi dello streaming

#### Analisi delle funzionalità di __Spark Streaming__
Analisi svolta in [`/src/main/scala/streaming/SparkStreaming.scala`](/src/main/scala/streaming/SparkStreaming.scala)
e si basa sul rispettivo container _docker_ presente in
[`/src/main/docker/stream-container`](/src/main/docker/stream-container).
__Istruzioni di lancio__:
- aggiungere la riga `127.0.0.1   localhost   stream` in `etc/hosts`
- spostarsi da terminale nella cartella [`/src/main/docker/stream-container`](/src/main/docker/stream-container)
- lanciare i comandi:
    - `docker build -t container-name .`
    - `docker run -ti --hostname stream -p 50070:50070 -p 9999:9999 -p 50075:50075 -p 50010:50010 container-name`
- lanciare il main di [`/src/main/scala/streaming/SparkStreaming.scala`](/src/main/scala/streaming/SparkStreaming.scala)

#### Analisi delle funzionalità di __Spark Structured Streaming__
Analisi svolta in [`/src/main/scala/streaming/SparkStreaming.scala`](/src/main/scala/streaming/SparkStreaming.scala)
e si basa sul rispettivo container _docker_ presente in
[`/src/main/scala/streaming/structuredStreaming/SparkStructuredStreaming.scala`](/src/main/scala/streaming/structuredStreaming/SparkStructuredStreaming.scala).
__Istruzioni di lancio__:
- aggiungere la riga `127.0.0.1   localhost   stream` in `etc/hosts`
- spostarsi da terminale nella cartella [`/src/main/docker/stream-container`](/src/main/docker/stream-container)
- lanciare i comandi:
    - `docker build -t container-name .`
    - `docker run -ti --hostname stream -p 50070:50070 -p 9999:9999 -p 50075:50075 -p 50010:50010 container-name`
- lanciare il main di [`/src/main/scala/streaming/structuredStreaming/SparkStructuredStreaming.scala`](/src/main/scala/streaming/structuredStreaming/SparkStructuredStreaming.scala)
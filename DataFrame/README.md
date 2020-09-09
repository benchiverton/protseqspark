# Spark

> **Apache Spark™** is a unified analytics engine for large-scale data processing.

https://spark.apache.org/

## Getting Started

### Installing JDK (Windows)

**JSK Version:** 14.0.2

1. Install the JDK by downloading your preferred package/archive/installer from https://www.oracle.com/java/technologies/javase-jdk14-downloads.html
2. Add a new system environment variable named `JAVA_HOME` with value `C:\Progra~1\Java\jdk-14.0.2`
3. Add `%JAVA_HOME%\bin` to the systems 'Path' environment variable
4. Verify instillation by running `javac --help`

### Installing Spark (Windows)

**Spark Version:** 3.0.0

**Package Type:** Pre-built for Apache Hadoop 2.7

1. Download the `.tgz` file using the settings listed above from https://spark.apache.org/downloads.html
2. Unzip the archive downloaded to `C:\Spark`
3. [SPARK-2356](https://issues.apache.org/jira/browse/SPARK-2356) - an existing bug means that you also need to copy winutils.exe into `C:\Spark\bin` (exe can be found in the ticket)
4. Add a new system environment variable named `SPARK_HOME` with value `C:\Spark`
5. Add a new system environment variable named `HADOOP_HOME` with value `C:\Spark`
6. Add `%SPARK_HOME%\bin` and `%HADOOP_HOME%\bin` to the systems 'Path' environment variable
7. Optional - update logging level of Spark
   1. Navigate to `C:\Spark\conf`
   2. Copy `log4j.properties.template` into the same directory
   3. Open `log4j.properties - Copy.template` and change all logging to `ERROR`
   4. Rename `log4j.properties - Copy.template` to `log4j.properties`
8. Verify instillation by running `spark-shell`

#### Spark Test Script

The following script will verify if Spark & Hadoop are working as intended. It counts each word in the Spark README file and writes its results to the directory `SparkTest/ReadMeWordCount`.

```
val a = sc.textFile("file:///Spark/README.md")
val b = a.flatMap(line => line.split(" "))
val c = b.map(word => (word, 1))
val d = c.reduceByKey((x, y) => x + y)
val e = d.sortBy(x => x._2, false)
e.saveAsTextFile("file:///SparkTest/ReadMeWordCount")
```


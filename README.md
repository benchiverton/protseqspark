![Test using pytest](https://github.com/benchiverton/protseqspark/workflows/Test%20using%20pytest/badge.svg)![Lint using Flake8](https://github.com/benchiverton/protseqspark/workflows/Lint%20using%20Flake8/badge.svg)

# Protein Sequences Spark Library

Python project for protein sequence analysis using [Apache Spark](https://spark.apache.org/).

## Getting Started

Checkout the repo and run the following commands in the base directory of the repo:

```bash
# create your virtual environment at the base of the repo
python -m virtualenv .venv
# activate your virtual environment
.venv\Scripts\activate
# install tools used for the setup script
pip install -U pytest setuptools wheel build
# install project dependencies
python setup.py install
# note: you may need to restart your IDE after these steps in order for intellisense to work

# deactivate virtual environment
deactivate
```

## System Requirements

#### Installing Python (Windows)

**Python Version:** 3.8.5

1. Download the installer from https://www.python.org/downloads/
2. Select 'Customize instillation'
3. Click 'Next'
4. Check '*Add Python to environment variables*' & set the install location to `C:\Python`, then click 'Install'
5. Verify Python instillation by running `python -V`
6. Verify Pip instillation by running `pip -V`

#### Installing JDK (Windows)

**JDK Version:** 14.0.2

1. Install the JDK by downloading your preferred package/archive/installer from https://www.oracle.com/java/technologies/javase-jdk14-downloads.html
2. Add a new system environment variable named `JAVA_HOME` with value `C:\Progra~1\Java\jdk-14.0.2`
3. Add `%JAVA_HOME%\bin` to the systems 'Path' environment variable
4. Verify instillation by running `javac --help`

#### Installing Spark (Windows)

**Spark Version:** 3.0.0

**Package Type:** Pre-built for Apache Hadoop 2.7

1. Download the `.tgz` file using the settings listed above from https://spark.apache.org/downloads.html
2. Unzip the archive downloaded to `C:\Spark`
3. [SPARK-2356](https://issues.apache.org/jira/browse/SPARK-2356) - an existing bug means that you also need to copy winutils.exe into `C:\Spark\bin` (exe can be found in the ticket)
4. Add a new system environment variable named `SPARK_HOME` with value `C:\Spark`
5. Add a new system environment variable named `HADOOP_HOME` with value `C:\Spark`
6. Add `%SPARK_HOME%\bin` and `%HADOOP_HOME%\bin` to the systems 'Path' environment variable
7. Optional - update logging level of Spark to `ERROR`
   1. Navigate to `C:\Spark\conf`
   2. Copy `log4j.properties.template` into the same directory
   3. Open `log4j.properties - Copy.template` and change all logging to `ERROR`
   4. Rename `log4j.properties - Copy.template` to `log4j.properties`
8. Verify instillation by running `spark-shell`

#### Spark Test Script

The following script is intended to be run through the Spark Shell and will verify if Spark is working as intended. It counts each word in the Spark README file and writes each word and it's count to `SparkTest/ReadMeWordCount`. 

```
val textFile = sc.textFile("file:///Spark/README.md")
val tokenisedFileData = textFile.flatMap(line => line.split(" "))
val countPrep = tokenisedFileData.map(word => (word, 1))
val counts = countPrep.reduceByKey((x, y) => x + y)
val sortedCounts = counts.sortBy(x => x._2, false)
sortedCounts.saveAsTextFile("file:///SparkTest/ReadMeWordCount")
```


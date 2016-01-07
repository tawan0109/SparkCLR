# Building SparkCLR

## Prerequisites

* JDK 7 or above.
* Maven 3.3.3 or above.
* Mono 4.2 stable or above. The download and installation instructions for Mono are available in [http://www.mono-project.com/download/#download-lin](http://www.mono-project.com/download/#download-lin).
* NuGet.

The following environment variables should be set properly:

* `JAVA_HOME`

## Instructions

* With `JAVA_HOME` set properly, navigate to [SparkCLR](./) directory: 

  ```  
  ./build.sh  
  ```

* Optional: 
  - Under [SparkCLR/scala](./scala) directory, run the following command to clean spark-clr*.jar built above: 

    ```  
    mvn clean
    ```  

  - Under [SparkCLR/csharp](./csharp) directory, run the following command to clean the .NET binaries built above:

    ```  
    ./clean.sh  
    ```  
    
[build.sh](build.sh) prepares the following directories under `SparkCLR\run` after the build is done:

  * **lib** ( `spark-clr*.jar` )  
  * **bin** ( `Microsoft.Spark.CSharp.Adapter.dll`, `CSharpWorker.exe`)  
  * **samples** ( The contents of `SparkCLR/csharp/Samples/Microsoft.Spark.CSharp/bin/Release/*`, including `Microsoft.Spark.CSharp.Adapter.dll`, `CSharpWorker.exe`, `SparkCLRSamples.exe`, `SparkCLRSamples.exe.Config` etc. ) 
  * **scripts** ( `sparkclr-submit.sh` )  
  * **data** ( `SparkCLR/csharp/Samples/Microsoft.Spark.CSharp/data/*` ) 


# Running Samples

## Prerequisites

JDK is installed, and the following environment variables should be set properly:

* `JAVA_HOME`

## Running in Local mode

With `JAVA_HOME` set properly, navigate to [SparkCLR](./) directory:

```  
./run-samples.sh  
```

It is **required** to run [build.sh](./build.sh) prior to running [run-samples.sh](./run-samples.sh).

[run-samples.sh](./run-samples.sh) downloads Apache Spark 1.4.1 and builds a customized version of Spark, sets up `SPARK_HOME` environment variable, points `SPARKCLR_HOME` to `SparkCLR/run` directory created by [Build.cmd](./build.cmd), and invokes [sparkclr-submit.sh](./scripts/sparkclr-submit.sh), with `spark.local.dir` set to `SparkCLR/run/Temp`.

**Note that SparkCLR requires a customized Apache Spark**. To build the customized Apache Spark, follow the steps below:

1. Download and unpack the binary package of Apache Spark 1.4.1.
2. Download and unpack the source package of Apache Spark 1.4.1, apply the diff patch [PythonWorkerFactory.scala.patch](./PythonWorkerFactory.scala.patch) on **core/src/main/scala/org/apache/spark/api/python/PythonWorkerFactory.scala**, and build Spark following the [instructions](http://spark.apache.org/docs/latest/building-spark.html).
3. Replace lib/spark-assembly\*hadoop\*.jar in the binary package with assembly/target/scala-2.10/spark-assembly\*hadoop\*.jar built in Step 2. Use/deploy this modified binary package for Spark.

A few more [run-samples.sh](./run-samples.sh) examples:
- To display all options supported by [run-samples.sh](./run-samples.sh): 

    ```  
    run-samples.sh  --help
    ```

- To run PiSample only:

    ```  
    run-samples.sh  --torun pi*
    ```

- To run PiSample in verbose mode, with all logs displayed at console:

    ```  
    run-samples.sh  --torun pi* --verbose
    ```

## Running in Standalone mode

```
sparkclr-submit.sh --verbose --master spark://host:port --exe SparkCLRSamples.exe  $SPARKCLR_HOME/samples sparkclr.sampledata.loc hdfs://path/to/sparkclr/sampledata
```
- When option `--deploy-mode` is specified with `cluster`, option `--remote-sparkclr-jar` is required and needs to be specified with a valid file path of spark-clr*.jar on HDFS.

## Running in YARN mode

```
sparkclr-submit.sh --verbose --master yarn-cluster --exe SparkCLRSamples.exe $SPARKCLR_HOME/samples sparkclr.sampledata.loc hdfs://path/to/sparkclr/sampledata
```

# Running Unit Tests

* Install NUnit Runner 3.0 or above using NuGet (see [https://www.nuget.org/packages/NUnit.Runners/](https://www.nuget.org/packages/NUnit.Runners/)), set `NUNITCONSOLE` to the path to nunit console, navigate to `SparkCLR/csharp` and run the following command:     
    ```
    ./test.sh
    ```

# Debugging Tips

CSharpBackend and C# driver are separately launched for debugging SparkCLR Adapter or driver.

For example, to debug SparkCLR samples:

* Launch CSharpBackend.exe using `sparkclr-submit.sh debug` and get the port number displayed in the console.  
* Navigate to `csharp/Samples/Microsoft.Spark.CSharp` and edit `App.Config` to use the port number from the previous step for `CSharpBackendPortNumber` config and also set `CSharpWorkerPath` config values.  
* Run `SparkCLRSamples.exe`.
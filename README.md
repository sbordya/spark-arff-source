## spark-arff-source

Instructions how to run this project (run this from the root of the repository):
1. Check the default configuration paths in `src/main/resources/application.conf` and overwrite if necessary:
```
input-path = "./input/*.arff"           // input path for arff files
output-path = "./output"                // output path for parquet files
```

2. Prepare a jar with dependencies (you'll need [sbt](https://www.scala-sbt.org/download.html) for this):
```bash
sbt assembly
```

3. Run the job to run the ingestion
```bash
java -cp target/scala-2.12/spark-arff-source-assembly-0.1.jar handson.Ingestion
```

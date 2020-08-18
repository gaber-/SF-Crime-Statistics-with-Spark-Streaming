# SF Crime Statistics with Spark Streaming
## Requirements

kafka-python
python 3.7
requirements.txt dependencies


## Screenshots

Kafka console:

![kafka console](https://github.com/gaber-/SF-Crime-Statistics-with-Spark-Streaming/blob/master/kafka_console.png)

Aggregate progress:

![aggregate](https://github.com/gaber-/SF-Crime-Statistics-with-Spark-Streaming/blob/master/aggregate.png)

Join progress:

![join](https://github.com/gaber-/SF-Crime-Statistics-with-Spark-Streaming/blob/master/join.png)

SparkUi:

![spark ui](https://github.com/gaber-/SF-Crime-Statistics-with-Spark-Streaming/blob/master/sparkUi.png)


## Optimization

### How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Lower spark.sql.shuffle.partitions greatly affects the latency, for small amount of data it is desirable to use a low number, as it leads to less overhead.

spark.default.parallelism affects throughput by allwing to use the power of the machine at its best  

### What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?


Spark UI provides a few metrics to monitor the performance of the streaming application, including timeline and duration of each job. 

The most effective parameter turned out to be "spark.sql.shuffle.partitions", as the default value is way too high for this application which has a relatively small amount of data.

Anther useful parameter turned out to be "spark.default.parallelism", which defaults to the number of cores available, but turned out to work best with 2 or 3 times that number for this application.

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 *
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * To run this example:
 *   `$ bin/run-example org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \
 *    zoo03 my-consumer-group topic1,topic2 1`
 */

public final class SparkJavaKafkaConsumer {
  private SparkJavaKafkaConsumer() {
  }

  @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
    // Create the context with 2 seconds batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));

    int numThreads = Integer.parseInt(args[3]);
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    String[] topics = args[2].split(",");
    for (String topic: topics) {
      topicMap.put(topic, numThreads);
    }

    JavaPairReceiverInputDStream<String, String> messages =
            KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }
    });
    
    //lines.dstream().saveAsTextFiles("kafka","spark"); //will create a kafka-***.spark folders for each interval and inside folder we have part* files
    
    //lines.repartition(1).dstream().saveAsTextFiles("kafka", "spark");

    /*JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      public Iterable<String> call(String x) {
        return Arrays.asList(SPACE.split(x));
      }
    });

    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });*/
    
    /*JavaRDD<String> alllines=lines.foreachRDD(new Function<JavaRDD<String>, String>() {
        public String call(JavaRDD<String> rdd) {
          return rdd.toString();
        }});*/
    //salllines.saveAsTextFile("/user/cloudera/kafkaoutput");
    
    lines.foreach(new Function<JavaRDD<String>, Void>() {
		public Void call(JavaRDD<String> rdd) throws Exception {
			if(!rdd.isEmpty())
				   rdd.saveAsTextFile("/user/murali/sparkjavakafkaconsumer");
			return null;
		}
    });
    
    

    lines.print();
    jssc.start();
    jssc.awaitTermination();
  }
}
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
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

public class LocalDataIngestionJava {

  public static void main(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
      System.exit(1);
    }

    //StreamingExamples.setStreamingLogLevels();
    SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
    //enabling WAL
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "false");
    
    // Create the context with 2 seconds batch size
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

    int numThreads = Integer.parseInt(args[3]);
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    String[] topics = args[2].split(",");
    for (String topic: topics) {
      topicMap.put(topic, numThreads);
    }

    JavaPairReceiverInputDStream<String, String> messages =
            KafkaUtils.createStream(jssc, args[0], args[1], topicMap);
    
    System.out.println("No Of Messages Received:"+messages.count());
    messages.print();
    
    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
        public String call(Tuple2<String, String> tuple2) {
          return tuple2._2();
        }
      });
    System.out.println("No Of lines Received:"+lines.count());
    
    JavaPairDStream<String, Integer> linesPair = lines.mapToPair(
  	      new PairFunction<String, String, Integer>() {
  	        @Override
  	        public Tuple2<String, Integer> call(String s) {
  	          return new Tuple2<String, Integer>(s, 1);
  	        }
  	      });
    System.out.println("No Of linesPair Received:"+linesPair.count());
     
    linesPair.print();
    linesPair.saveAsNewAPIHadoopFiles("spark", "kafka", Text.class, IntWritable.class, TextOutputFormat.class);
 
    jssc.start();
    jssc.awaitTermination();
  }
}
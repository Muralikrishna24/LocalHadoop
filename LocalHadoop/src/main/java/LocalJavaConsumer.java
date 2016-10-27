import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

    public class LocalJavaConsumer {
       private ConsumerConnector consumerConnector = null;
       private final String topic = "ata1";

       public void initialize() {
             Properties props = new Properties();
             props.put("zookeeper.connect", "localhost:2181");
             props.put("group.id", "hadoop");
             /*props.put("zookeeper.session.timeout.ms", "400");
             props.put("zookeeper.sync.time.ms", "300");
             props.put("auto.commit.interval.ms", "1000");*/
             ConsumerConfig conConfig = new ConsumerConfig(props);
             consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
       }

       public void consume() throws IOException {
    	   //File file = new File("C:\\Murali\\Sabre\\Hello1.txt");
 	      
 	      // creates the file
 	      //file.createNewFile();
    	   //int count=0;
             //Key = topic name, Value = No. of threads for topic
             Map<String, Integer> topicCount = new HashMap<String, Integer>();       
             topicCount.put(topic, new Integer(1));
            
             //ConsumerConnector creates the message stream for each topic
             Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                   consumerConnector.createMessageStreams(topicCount);         
            
             // Get Kafka stream for topic 'mytopic'
             List<KafkaStream<byte[], byte[]>> kStreamList =
                                                  consumerStreams.get(topic);
             // Iterate stream using ConsumerIterator
             for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
                    ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();
                   
                    while (consumerIte.hasNext())
                    {
                    	   String message=new String(consumerIte.next().message());
                           System.out.println(message);
                           //write(message,file);
                           //count++;
                           //System.out.println("message count:"+count);
                    }
                    //System.out.println("No of messages consumed:"+count);
             }
             //Shutdown the consumer connector
             if (consumerConnector != null)   consumerConnector.shutdown();          
       }

       /*private void write(String message,File file) throws IOException {
   	      
    	      // creates a FileWriter Object
    	      FileWriter writer = new FileWriter(file); 
    	      
    	      // Writes the content to the file
    	      writer.write(message); 
    	      writer.flush();
    	      writer.close();
	}*/

	public static void main(String[] args) throws InterruptedException, IOException {
		
             LocalJavaConsumer localJavaConsumer = new LocalJavaConsumer();
             // Configure Kafka consumer
             localJavaConsumer.initialize();
             // Start consumption
             localJavaConsumer.consume();
       }
   }
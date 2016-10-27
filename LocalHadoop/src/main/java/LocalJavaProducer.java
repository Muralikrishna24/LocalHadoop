import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.javaapi.producer.Producer;

public class LocalJavaProducer {
String TOPIC = null;
int count=0;
BufferedReader br=null;

//declaring Properties object to set kafka producer properties	
Properties properties=null;
//declaring the ProducerConfig object
ProducerConfig producerConfig=null;
//declaring Producer object used to send messages to kafka topic
Producer<String,String> producer=null;

//declaring BufferedReader object

public void intialize(String topicName)
{
	TOPIC=topicName;
	properties = new Properties();
	properties.put("metadata.broker.list","localhost:9092"); //to set the list of brokers as comma seperated
	properties.put("producer.type", "sync");
	properties.put("request.required.acks", "1");
	properties.put("serializer.class","kafka.serializer.StringEncoder"); // string data passing
	producerConfig = new ProducerConfig(properties);
	producer = new Producer<String, String>(producerConfig);
	
}

private void listFiles(String directoryName) throws IOException, InterruptedException{
    File directory = new File(directoryName);
    //get all the files from a directory
    File[] fList = directory.listFiles();
    for (File file : fList){
        sendMessage(file);
    }
    if(br!=null)
    br.close();
    System.out.println("Total messages sent:"+count);
    System.out.println("Producer Ended successfully");
    producer.close();
}
private void sendMessage(File file) throws IOException, InterruptedException {
		System.out.println("Inside Send Message");	
		String line=null;
		
		br = new BufferedReader(new FileReader(file));
		while ((line = br.readLine()) != null) {
			KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC,line);//topic,message
			producer.send(message);
			Thread.sleep(100);
			count++;
			System.out.println("Message:"+message);
		}
}

	public static void main(String[] args) throws IOException, InterruptedException{
		LocalJavaProducer localJavaProducer=new LocalJavaProducer();
		if(args.length!=2)
		{
			System.out.println("Please pass topicname and location of files parameters");
		}
		localJavaProducer.intialize(args[0]);
		localJavaProducer.listFiles(args[1]);
	}
}
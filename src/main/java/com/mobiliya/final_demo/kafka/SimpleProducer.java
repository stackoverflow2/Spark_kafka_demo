package final_demo;

//import util.properties packages
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
//Create java class named “SimpleProducer”

public class SimpleProducer {

    public static void main(String[] args) throws Exception {

        //Assign topicName to string variable
        String topicName = "test";

        // create instance for properties to access producer configs   
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "192.168.1.7:9092");

        //Set acknowledgements for producer requests.      
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0   
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.   
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("serializer.class", "kafk.serializer.StringEncoder");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        List<String> lines = CSVReaderInJava.readBooksFromCSV("C:\\Users\\Vaibhav\\Downloads\\Compressed\\splitcsv-9605c0bc-d274-4092-bdaf-c7b23025c5a3-results\\1.csv");
        
        lines.remove(0);
        lines.remove(1);
        
        long count=0;
        // let's print all the person read from CSV file
        for (String b : lines) {
            count++;
            producer.send(new ProducerRecord<String, String>(topicName, "Training", b));
            //System.out.println(b);
        }
System.out.println("Message sent successfully"+count);
        System.out.println("Message sent successfully");
        producer.close();
    }

}

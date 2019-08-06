package com.mobiliya.final_demo.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobiliya.final_demo.debezium.domain.key.Key;
import com.mobiliya.final_demo.debezium.domain.val.Payload;
import com.mobiliya.final_demo.debezium.domain.val.Event;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MyClass {
    private KafkaConsumer<String, JsonNode> kc;
    private String topic = null;
    private String pollTimeout = null;

    private ObjectMapper mapper = new ObjectMapper();

    
    public void  myfn() {
    	
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafEEne-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");

        topic = "dbserver1.inventory.customers";

        kc = new KafkaConsumer<>(consumerProps);
        kc.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);

        pollTimeout = System.getenv().getOrDefault("KAFKA_CONSUMER_POLL_TIMEOUT", "3000"); //default is 3 seconds
        
        
        while(true) {
            ConsumerRecords<String, JsonNode> records = kc.poll(Long.valueOf(pollTimeout)); // timeout
            System.out.println("Got " + records.count() + " records");

            for (ConsumerRecord<String, JsonNode> record : records) {
                try {
                    Event event = mapper.readValue(record.value().toString(), Event.class);
                    Key key = mapper.readValue(record.key(), Key.class);
                    Payload payload = event.getPayload();
                    if(payload!=null){
                        System.out.println("Record with ID "+key.getPayload().getId() + " was "+ watHappened(payload));
                    }
                    
                } catch (IOException ex) {
                    Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        

    	
    }
    

    
    private String watHappened(Payload payload){
        String action = null;
        
        if(payload.getOp().equals("c")){
            action = "Created";
        }else if(payload.getOp().equals("u")){
            action = "Updated";
        }else if(payload.getOp().equals("d")){
            action = "Deleted";
        }
        
        return action;
    }
	
    
    public static void main(String args[]) {
    	
    	MyClass class1= new MyClass();
    	class1.myfn();
    	
    }


}

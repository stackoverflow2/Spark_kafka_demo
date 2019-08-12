package com.mobiliya.final_demo.avro;

import com.mobiliya.Transaction;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class AvroProducer {

    public static void main(String[] args) {

        String line,topic,fileName;
        long count=0;

        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        Producer<String, Transaction> producer = new KafkaProducer<String, Transaction>(properties);

        topic = "test";
        fileName = "/home/rameshwar/Downloads/paysim1/data.csv";
        //reading from CSV file and sending data
        Path pathToFile = Paths.get(fileName);

        try (BufferedReader br = Files.newBufferedReader(pathToFile,
                StandardCharsets.UTF_8)) {

            // read the first line from the text file
            line = br.readLine();

            // loop until all lines are read

            while (line != null) {
                Transaction transaction=null;
                String[] attributes = line.split(",");
                try {
                    transaction = Transaction.newBuilder()
                            .setStep(Integer.parseInt(attributes[0]))
                            .setType(attributes[1])
                            .setAmount(Float.parseFloat(attributes[2]))
                            .setNameOrig(attributes[3])
                            .setOldbalanceOrg(Float.parseFloat(attributes[4]))
                            .setNewbalanceOrig(Float.parseFloat(attributes[5]))
                            .setNameDest(attributes[6])
                            .setOldbalanceDest(Float.parseFloat(attributes[7]))
                            .setNewbalanceDest(Float.parseFloat(attributes[8]))
                            .setIsFraud(Integer.parseInt(attributes[9]))
                            .setIsFlaggedFraud(Integer.parseInt(attributes[10]))
                            .build();

                    // copied from avro examples
                    ProducerRecord<String, Transaction> producerRecord = new ProducerRecord<String, Transaction>(
                            topic, transaction
                    );
                    System.out.println("Hi"+count);
                    count++;

                    producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                //System.out.println(metadata);
                            } else {
                                System.out.println("ERROR: "+exception.getMessage());
                                exception.printStackTrace();
                            }
                        }
                    });

                }catch (Exception e) {
                    System.out.println("ERROR: "+e.getMessage());
                }
                line = br.readLine();
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        System.out.println(count);
        producer.flush();
        producer.close();

    }
}
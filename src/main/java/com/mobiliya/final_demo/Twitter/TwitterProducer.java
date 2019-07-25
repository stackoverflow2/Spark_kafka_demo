package com.mobiliya.final_demo.Twitter;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Rameshwar
 */


import java.util.*;
import twitter4j.*;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;


import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class TwitterProducer {
   public static Producer<String, String> producer;
   public static String topicName="";
    
    public static void InitializeProducer(){
    
         topicName = "test";
         Properties props = new Properties();
         props.put("bootstrap.servers", "localhost:9092");
         props.put("acks", "all");
         props.put("retries", 0);
         props.put("batch.size", 16384);
         props.put("linger.ms", 1);
         props.put("buffer.memory", 33554432);
      props.put("key.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer"); 
      producer = new KafkaProducer<String, String>(props);
      
      
    }
    
    public static void sendEvent(long key,String value){
    
        
         producer.send(new ProducerRecord<String, String>(topicName,Long.toString(key), value));
        
    }
    
    public static void main(String[] args) throws TwitterException, InterruptedException {
        
       
        // initilaizing the producer
        InitializeProducer();
        
       
    String twitterConsumerKey="APGkKdqorgTac02lVZmKLsWyl";
    String twitterConsumerSecret="iKM4hwt1o0oHkBUiPwwHBzTKAVxpcsW2KrePLglcj3SGGQ5rHT";
    String twitterOauthAccessToken="366984933-uP7g6YtEK36DBlyWxSTe2fxyfxp9Cgaeb83PMk2u";
    String twitterOauthTokenSecret="1IF5OJJ6ileUR8KWRine6jh8Ua01AkoKDCrJisvtPjdEn";
       
    
    
    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setDebugEnabled(true)
  .setOAuthConsumerKey(twitterConsumerKey)
  .setOAuthConsumerSecret(twitterConsumerSecret)
  .setOAuthAccessToken(twitterOauthAccessToken)
  .setOAuthAccessTokenSecret(twitterOauthTokenSecret);
  
    TwitterFactory twitterFactory = new TwitterFactory(cb.build());    
    
    twitter4j.Twitter twitter = twitterFactory.getInstance();

  
    
            
   Query query = new Query("#IndvsNZ");
   query.lang("en");
   Boolean finished = false;
   while (!finished) {
   QueryResult result = twitter.search(query); 
   List<Status> statuses = result.getTweets();
   long lowestStatusId = Long.MAX_VALUE;
   for (Status status : statuses) {
  
      sendEvent(status.getId(),status.getText());
        System.out.println(status.getText());
    lowestStatusId = Math.min(status.getId(), lowestStatusId);
  }
  query.setMaxId(lowestStatusId - 1);
}
    }
    
}

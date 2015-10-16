/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package irckafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
    
    ProducerConfig producerConfig;
    kafka.javaapi.producer.Producer<String,String> producer;
    
    public KafkaProducer(){
        Properties properties = new Properties();
        properties.put("metadata.broker.list","localhost:9092");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        producerConfig = new ProducerConfig(properties);
        producer = new kafka.javaapi.producer.Producer<>(producerConfig);
    }
    
    public void sendMessage(String channelName, String text, String nickname) {
        KeyedMessage<String, String> message =new KeyedMessage<>(channelName, "["+ channelName + "]("+ nickname +") : " + text);
        producer.send(message);
    }
    
    public void closeConnection(){
        producer.close();
    }
}

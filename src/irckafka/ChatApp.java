/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package irckafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.apache.commons.lang3.RandomStringUtils;

public class ChatApp {
    
    public static String nickname = "unknown";
    public static String groupId;
    
    public static Map<String, KafkaConsumer> channelGroup = new HashMap<>();
    
    public static void main(String[] args) {
        
        groupId = RandomStringUtils.random(8, true, true);
        
        boolean isOnline = true;
        
        Scanner in = new Scanner(System.in);
        while(isOnline) {
            
            String[] input = in.nextLine().split(" ");
            String command = input[0].toLowerCase();
            
            switch (command) {
                case "/nick":
                    if(input.length >= 2){
                        nickname = input[1];
                        System.out.println("your nickname is " + nickname);
                    }
                    else{
                        System.out.println("write nick name please!");
                    }
                    break;
                case "/join":
                    if(input.length >= 2){
                        KafkaConsumer consume = new KafkaConsumer(groupId, input[1]);
                        consume.start();
                        
                        channelGroup.put(input[1], consume);
                    }
                    else{
                        System.out.println("write channel name please!");
                    }
                    break;
                case "/leave":
                    if(input.length >= 2){
                        if(channelGroup.containsKey(input[1])){
                            KafkaConsumer consume = channelGroup.get(input[1]);
                            consume.shutdown();
                            channelGroup.remove(input[1]);
                            System.out.println("your leave channel " + input[1]);
                        }
                        else{
                            System.out.println("wrong channel name!!");
                        }
                    }
                    else{
                        System.out.println("write channel name please!");
                    }
                    break;
                case "/exit":
                    System.out.println("bye");
                    System.exit(0);
                    break;
                default:    //broadcast
                    if(command.contains("@")) {
                        String channelname = command.substring(1);
                        KafkaProducer produce = new KafkaProducer();
                        if(channelGroup.containsKey(input[1])){  
                            String message = "";
                            for(int i=1; i<input.length; i++) {
                                message+=input[i]+" ";
                            }
                            produce.sendMessage(channelname, message, nickname);
                        }
                        produce.closeConnection();
                    }
                    else {
                        String message = "";
                        for(int i=0; i<input.length; i++) {
                            message+=input[i]+" ";
                        }
                        
                        KafkaProducer produce = new KafkaProducer();
                        for(String cList : channelGroup.keySet()){
                            produce.sendMessage(cList, message, nickname);
                        }
                        produce.closeConnection();
                    }
            }
        }
    }
}

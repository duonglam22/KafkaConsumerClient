/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.media.vnptpay.core.kafka.consumer;

import static com.media.vnptpay.utils.Constants.LOWBALANCE_ENCODE_QUEUE;
import com.media.vnptpay.utils.Params;
import java.time.Duration;
import java.util.Calendar;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/**
 *
 * @author MSI
 */
public class TopupLowbalanceConsumer extends Thread{
    
    static final Logger logger = LogManager.getLogger(TopupLowbalanceConsumer.class);
    
    private static String bootstrapServer;
    private static String groupId;
    private static String topicLowbalance;
    private static String OFFSET_RESET_EARLIER;
    private static int MAX_POLL_RECORDS_CONFIG;
    private static KafkaConsumer<String, String> consumer = null;
    private long totalMsg;
    private String startTime;
    
    public TopupLowbalanceConsumer() {
        logger.info("init TopupLowbalanceConsumer object!");
        bootstrapServer = Params.KAFKA_BROKER;
        groupId = "vnpt-pay";
        topicLowbalance = "lowbalance-5k";
        OFFSET_RESET_EARLIER = "earliest";
        MAX_POLL_RECORDS_CONFIG = 500;
        totalMsg = 0;
        startTime = Calendar.getInstance().getTime().toString();
        createConsumer();
    }
    
    @Override
    public void run() {
        try {
            while(true) {
                try{
//                    logger.info("test consumer polling...");
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    int size = records.count();
                    logger.info("consumer polling data size: {}", size);
                    if(size > 0) {
                        totalMsg += size;
                        logger.info("total message polled: {} from {}", totalMsg, startTime);
                    }
                    for(ConsumerRecord<String, String> record: records) {
                        logger.info("key: {}, Value: {}, Partition: {}, Offset: {}", record.key(), record.value(), record.partition(), record.offset());
                        String data = record.value();
                        if(!data.isEmpty()) {
                            LOWBALANCE_ENCODE_QUEUE.add(data);
                        }
                    }
                    logger.info("after polling LOWBALANCE_ENCODE_QUEUE size: {}", LOWBALANCE_ENCODE_QUEUE.size());
                    consumer.commitAsync();
                }
                catch(Exception ex) {
                    logger.error("occurs exception: {}", ex.toString());
                }
                finally {
                    try {
                        Thread.sleep(20 * 1000); //10s
                    } catch (InterruptedException ex) {
                        logger.error("occurs exception: {}", ex.toString());
                    }
                }
            }
        }
        catch(Exception ex) {
            logger.error("occurs exception: {}", ex.toString());
        }
        finally {
            consumer.close();
        }
    }
    
    public static void createConsumer() {
        logger.info("create new kafka consumer");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS_CONFIG);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicLowbalance));
        logger.info("kafka consumer info: {bootstrapServer: {}, groupId: {}, topic: {}}", bootstrapServer, groupId, topicLowbalance);
    }
    
}

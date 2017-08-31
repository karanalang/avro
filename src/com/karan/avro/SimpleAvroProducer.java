package com.karan.avro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class SimpleAvroProducer {
    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\" },"
            + "  { \"name\":\"str2\", \"type\":\"string\" },"
            + "  { \"name\":\"int1\", \"type\":\"int\" }"
            + "]}";

   public static void main(String[] args) throws InterruptedException {
        java.util.Properties props = new Properties();
        props.put("bootstrap.servers", "nwk2-bdp-kafka-04.gdcs-qa.apple.com:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        //props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        //props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        
        org.apache.avro.Schema.Parser parser = new Schema.Parser();
        org.apache.avro.Schema schema = parser.parse(USER_SCHEMA);
        
        //Using Twitter bijection API instead of the Avro API
        com.twitter.bijection.Injection<org.apache.avro.generic.GenericRecord, byte[]> recordInjection = com.twitter.bijection.avro.GenericAvroCodecs.toBinary(schema);

        System.out.println("create com.twitter.bijection.Injection " + recordInjection);
        
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        System.out.println("create KafkaProducer");
        
        for (int i = 0; i < 1000; i++) {
            org.apache.avro.generic.GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("str1", "Str 1-" + i);
            avroRecord.put("str2", "Str 2-" + i);
            avroRecord.put("int1", i);
            
            System.out.println(" get Generic Avro Record " + " <- i -> " + avroRecord);

            byte[] bytes = recordInjection.apply(avroRecord);

            org.apache.kafka.clients.producer.ProducerRecord<String, byte[]> record = new ProducerRecord<>("AvroTest", bytes);
            producer.send(record);

            System.out.println(" sent record " + i + " <-> " + bytes);
            
            Thread.sleep(250);
        }
        producer.close();
    }
}
package com.chan.common.utils;

import com.chan.common.constant.PropertiesConstants;
import com.chan.common.entity.Student;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerUtil {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterToolUtil.createParameterTool();
        Properties properties = new Properties();
        properties.put(PropertiesConstants.BOOTSTRAP_SERVERS, parameterTool.get(PropertiesConstants.KAFKA_BROKERS));
        properties.put(PropertiesConstants.KEYSERIALIZER, PropertiesConstants.STRINGSERIALIZER);
        properties.put(PropertiesConstants.VALUESERIALIZER, PropertiesConstants.STRINGSERIALIZER);

        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        for(int i=0; i<10;i++){
            Student student = new Student(i, PropertiesConstants.CY + i, PropertiesConstants.PASSWORD + i, PropertiesConstants.AGE + i);
            /*
             * A key/value pair to be sent to Kafka. This consists of a topic name to which the record is being sent, an optional
             * partition number, and an optional key and value.
             */
            ProducerRecord record = new ProducerRecord<String, String>(PropertiesConstants.STUDENT_TOPIC, null, null, GsonUtil.toJson(student));
            kafkaProducer.send(record);
        }

        kafkaProducer.flush();

    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.plesba.kafkamanager;

import com.plesba.kafkaworkers.target.KafkaProducerFileSource;
import com.plesba.kafkaworkers.source.KafkaConsumerFileTarget;
import com.plesba.kafkamanager.utils.KMProperties;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author renee
 */
public class KafkaManager {

    private static String propertiesFile = null;
    private static Properties kafkaMgrProps = null;
    private static KafkaProducerFileSource kfWriter = null;
    private static KafkaConsumerFileTarget kfReader = null;
    private static Properties kfwProp;
    private static Properties kfrProp;
    private static String datasource;
    private static String datatarget;

    private static final Log LOG = LogFactory.getLog(KafkaManager.class);

    public static void main(String[] args) throws IOException {

        LOG.info("KafkaManager starting main........");

        //setup properties

        if (args.length == 1) {
            propertiesFile = args[0];
            LOG.info("KafkaManager Properties file: " + propertiesFile);
        } else {

            LOG.info("KafkaManager <propertiesFile>" + "Usage: java " + KafkaManager.class.getName());
            System.exit(1);
        }

        kafkaMgrProps = new KMProperties(propertiesFile).getProp();

        datasource = kafkaMgrProps.getProperty("km.infilename");
        datatarget = kafkaMgrProps.getProperty("km.outfilename");

        LOG.info("KafkaManager datasource =  " + datasource);
        LOG.info("KafkaManager datatarget =  " + datatarget);


        //kafka consumer, read from kafka stream / write to target (file, db, etc.)
        LOG.info("KafkaManager input from stream via KafkaConsumerFileTarget (consumer). Target is file. ");

        kfrProp = new Properties();
        kfrProp.setProperty("client.id", kafkaMgrProps.getProperty("kafka.client.id"));
        kfrProp.setProperty("acks", kafkaMgrProps.getProperty("kafka.acks"));
        kfrProp.setProperty("bootstrap.servers", kafkaMgrProps.getProperty("kafka.bootstrap.servers"));
        kfrProp.setProperty("topic", kafkaMgrProps.getProperty("kafka.topic"));
        kfrProp.setProperty("key.deserializer", kafkaMgrProps.getProperty("kafka.key.deserializer.class"));
        kfrProp.setProperty("value.deserializer", kafkaMgrProps.getProperty("kafka.value.deserializer.class"));
        kfrProp.setProperty("group.id", kafkaMgrProps.getProperty("kafka.group_id_config"));
        try {
            kfReader = new KafkaConsumerFileTarget(kfrProp, datatarget);
            new Thread(
                    new Runnable() {
                        public void run() {
                            kfReader.processData();
                        }
                    }
            ).start();
        } catch (Exception ex) {
            Logger.getLogger(KafkaConsumerFileTarget.class.getName()).log(Level.SEVERE, null, ex);
        }

        //kafka producer, read from file / write to kafka stream (producer)
        LOG.info("KafkaManager output to stream via KafkaProducerFileSource (producer). Source is file. ");

        kfwProp = new Properties();
        kfwProp.setProperty("client.id", kafkaMgrProps.getProperty("kafka.client.id"));
        kfwProp.setProperty("acks", kafkaMgrProps.getProperty("kafka.acks"));
        kfwProp.setProperty("bootstrap.servers", kafkaMgrProps.getProperty("kafka.bootstrap.servers"));
        kfwProp.setProperty("topic", kafkaMgrProps.getProperty("kafka.topic"));
        kfwProp.setProperty("key.serializer", kafkaMgrProps.getProperty("kafka.key.serializer.class"));
        kfwProp.setProperty("value.serializer", kafkaMgrProps.getProperty("kafka.value.serializer.class"));
        kfwProp.setProperty("producer.type", kafkaMgrProps.getProperty("kafka.producer.type"));
        kfrProp.setProperty("filesource", datasource);

        try {
            kfWriter = new KafkaProducerFileSource(kfwProp, datasource);
            kfWriter.processData();
        } catch (InterruptedException ex) {
            Logger.getLogger(KafkaProducerFileSource.class.getName()).log(Level.SEVERE, null, ex);
        }

        LOG.info("KafkaManager Completed................");
    }


}
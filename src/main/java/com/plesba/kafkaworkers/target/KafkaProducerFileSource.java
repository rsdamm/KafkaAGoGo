package com.plesba.kafkaworkers.target;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.net.InetAddress;

//Producer: read file; write to kafka stream
public class KafkaProducerFileSource {

    private int recordCount = 0;

    private static final String DEFAULT_ACKS = "1";
    private static final String DEFAULT_CLIENTID = "localhost";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TOPIC = "test";
    private static final String DEFAULT_KEY_SERIALIZER = "defaultkey";
    private static final String DEFAULT_VALUE_SERIALIZER = "defaultvalue";
    private static final String DEFAULT_PRODUCER_TYPE = "sync";

    private static String acks = DEFAULT_ACKS;
    private static String clientId = DEFAULT_CLIENTID;
    private static String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    private static String topic = DEFAULT_TOPIC;
    private static String keySerializer = DEFAULT_KEY_SERIALIZER;
    private static String valueSerializer = DEFAULT_VALUE_SERIALIZER;
    private static String producerType = DEFAULT_PRODUCER_TYPE;

    private com.opencsv.CSVReader reader = null;
    private static String csvFileToRead = null;
    private String [] nextLine = null;
    private byte[] theByteArray = null;
    private StringBuilder recordStringBuffer;
    private int i=0;

    private boolean stopProcessing = false;
    private Producer<String, String> producer;
    private static final Log LOG = LogFactory.getLog(KafkaProducerFileSource.class);

    public KafkaProducerFileSource(Properties parameterProperties, String parameterFileToRead) throws InterruptedException {

        LOG.info("KafkaProducerFileSource (producer) started processing.");

        csvFileToRead = parameterFileToRead;

        String acksOverride = parameterProperties.getProperty("acks");
        if (acksOverride != null) {
            acks = acksOverride;
        }

        LOG.info("KafkaProducerFileSource using acks " + acks);

        String clientIdOverride = parameterProperties.getProperty("client.id");
        if (clientIdOverride != null) {
            clientId = clientIdOverride;
        } else {
            try {
                clientId = InetAddress.getLocalHost().getHostName();
            } catch (Exception ex) {
                Logger.getLogger(KafkaProducerFileSource.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        LOG.info("KafkaProducerFileSource using clientId " + clientId);

        String bootstrapserversOverride = parameterProperties.getProperty("bootstrap.servers");
        if (bootstrapserversOverride != null) {
            bootstrapServers = bootstrapserversOverride;
        }

        LOG.info("KafkaProducerFileSource using bootstrapservers " + bootstrapServers);


        String topicOverride = parameterProperties.getProperty("topic");
        if (topicOverride != null) {
            topic = topicOverride;
        }

        LOG.info("KafkaProducerFileSource using topic " + topic);

        String keySerializerOverride = parameterProperties.getProperty("key.serializer");
        if (keySerializerOverride != null) {
            keySerializer = keySerializerOverride;

        }

        LOG.info("KafkaProducerFileSource using key serializer " + keySerializer);

        String valueSerializerOverride = parameterProperties.getProperty("value.serializer");
        if (valueSerializerOverride != null) {
            valueSerializer = valueSerializerOverride;

        }

        LOG.info("KafkaProducerFileSource using value serializer " + valueSerializer);

        String producerTypeOverride = parameterProperties.getProperty("producer.type");
        if (producerTypeOverride != null) {
            producerType = producerTypeOverride;

        }

        LOG.info("KafkaProducerFileSource using producer type (sync vs async) " + producerType);


        producer = new KafkaProducer<String, String>(parameterProperties);

        LOG.info("KafkaProducerFileSource created Kafka Producer ");

        csvreaderSetup();

        LOG.info("KafkaProducerFileSource setup csvreader  ");

    }

    public void csvreaderSetup() {

        LOG.info("CSVSourceToStream started processing.");
        try {
            //Get the CSVSourceToStream instance specifying the delimiter to be used
            reader = new com.opencsv.CSVReader(new FileReader(csvFileToRead), ',');
        } catch (IOException e) {
            System.err.println(e);
        }

    }

    public int getLoadedCount() {
        return this.recordCount;
    }

    public void processData() throws RuntimeException {

        LOG.info("KafkaProducerFileSource started stream processing to topic: " + topic);
        LOG.info("KafkaProducerFileSource mode: " + producerType);

        StringBuilder recordStringBuffer = new StringBuilder();
        String streamRecord = new String();

        try {
            while ((nextLine = reader.readNext()) != null) {
                for (i = 0; i < nextLine.length; i++) {

                    if (i > 0) {
                        recordStringBuffer.append(",");
                    }

                    recordStringBuffer.append(nextLine[i]);

                }
                streamRecord = recordStringBuffer.toString() + '\n';
                ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, streamRecord);
                LOG.info("KafkaProducerFileSource record to put placed on kafka stream: " + streamRecord);
                try {
                    if (producerType.equals("sync")) { //sync
                        producer.send(rec);
                    } else producer.send(rec, new producerCallback()); //async
                } catch (Exception ex) {
                    Logger.getLogger(KafkaProducerFileSource.class.getName()).log(Level.SEVERE, null, ex);
                }

                LOG.info("KafkaProducerFileSource writing record to stream---> " + recordStringBuffer);
                recordCount++;
                recordStringBuffer.setLength(0);
            }
            LOG.info("KafkaProducerFileSource records written to stream " + producerType + " : " + recordCount);

        } catch (Exception ex) {

            LOG.error("KafkaProducerFileSource error detected in processData", ex);

        } finally {
            producer.close();
        }

        LOG.info("KafkaProducerFileSource (Producer) finished processing");
    }

    class producerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null)
                System.out.println("KafkaProducerFileSource async failed with an exception");
            else
                System.out.println("KafkaProducerFileSource async call successful");
        }
    }
}

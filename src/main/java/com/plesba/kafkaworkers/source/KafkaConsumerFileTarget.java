package com.plesba.kafkaworkers.source;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;


import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

//reads kafka stream writes to output file
public class KafkaConsumerFileTarget {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_GROUP_ID_CONFIG = "rsdKFGroup1";
    private static final String DEFAULT_KEY_DESERIALIZER = "defaultkey";
    private static final String DEFAULT_VALUE_DESERIALIZER = "defaultvalue";
    private static final int DEFAULT_MAX_RECORDS_TO_PROCESS = 1;
    private static final String DEFAULT_TOPIC = "test";

    private static String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    private static String groupIdConfig = DEFAULT_GROUP_ID_CONFIG;
    private static Integer maxRecordsToProcess = DEFAULT_MAX_RECORDS_TO_PROCESS;
    private static String keyDeSerializer = DEFAULT_KEY_DESERIALIZER;
    private static String topic = DEFAULT_TOPIC;
    private static String valueDeserializer = DEFAULT_VALUE_DESERIALIZER;
    private final Consumer<Long, String> consumer;

    private StringBuilder recordStringBuffer;
    private int recordCount = 0;
    private boolean stopProcessing = false;
    private String csvFileOut = null;
    private FileWriter fileWriter;

    private static final Log LOG = LogFactory.getLog(KafkaConsumerFileTarget.class);

    public KafkaConsumerFileTarget(Properties parameterProperties, String parameterFileToWrite) {

        LOG.info("KafkaConsumerFileTarget Constructor started");

        csvFileOut = parameterFileToWrite;
        LOG.info("KafkaConsumerFileTarget writing to " + csvFileOut);

        recordStringBuffer = new StringBuilder();


        LOG.info("KafkaConsumerFileTarget (consumer) started processing.");


        String bootstrapserversOverride = parameterProperties.getProperty("bootstrap.servers");
        if (bootstrapserversOverride != null) {
            bootstrapServers = bootstrapserversOverride;
        }

        LOG.info("KafkaConsumerFileTarget using bootstrapservers " + bootstrapServers);

        String maxRecordsToProcessOverride = parameterProperties.getProperty("maxrecordstoprocess");
        if (maxRecordsToProcessOverride != null) {
            maxRecordsToProcess = Integer.parseInt(maxRecordsToProcessOverride);

        }
        LOG.info("KafkaConsumerFileTarget using maxrecordstoprocess " + maxRecordsToProcess);

        String groupIdConfigOverride = parameterProperties.getProperty("group_id");
        if (groupIdConfigOverride != null) {
            groupIdConfig = groupIdConfigOverride;

        }
        LOG.info("KafkaConsumerFileTarget using groupId " + groupIdConfig);


        String keyDeserializerOverride = parameterProperties.getProperty("key.deserializer");
        if (keyDeSerializer != null) {
            keyDeSerializer = keyDeserializerOverride;

        }

        LOG.info("KafkaConsumerFileTarget using key deserializer " + keyDeSerializer);

        String valueDeserializerOverride = parameterProperties.getProperty("value.deserializer");
        if (valueDeserializerOverride != null) {
            valueDeserializer = valueDeserializerOverride;

        }

        LOG.info("KafkaConsumerFileTarget using value deserializer " + valueDeserializer);

        String topicOverride = parameterProperties.getProperty("topic");
        if (topicOverride != null) {
            topic = topicOverride;
        }

        LOG.info("KafkaConsumerFileTarget using topic " + topic);

        //create the consumer
        consumer = new KafkaConsumer(parameterProperties);
        LOG.info("KafkaConsumerFileTarget Consumer created");


    }

    public int getReadCount() {
        return this.recordCount;
    }

    public void processData() {

        LOG.info("KafkaConsumerFileTarget (consumer) processData started");

        //subscribe to topic
        consumer.subscribe(Collections.singletonList(topic));


        LOG.info("KafkaConsumerFileTarget (consumer) topic subscribed");

        Duration d = Duration.ofSeconds(10);

        try {
            fileWriter = new FileWriter(csvFileOut);
        }  catch (IOException e) {
            LOG.info("KafkaConsumerFileTarget IOException creating fileWriter");
            e.printStackTrace();
        }

            LOG.info("KafkaConsumerFileTarget (consumer) starting loop");

            while (true) {
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(d);


                LOG.info("KafkaConsumerFileTarget (consumer) polled");


                consumerRecords.forEach(record -> {
                   // putDataToFile("Key: " + record.key() + " Value: " + record.value() + " Partition: " + record.partition() + " Offset: " + record.offset());
                    putDataToFile(record.value());
                });

                if (consumerRecords.count() > 0) {
                    if (recordCount > maxRecordsToProcess & maxRecordsToProcess > -1) break;
                    else continue;
                }
                consumer.commitAsync();
            }
            consumer.close();

            try {

                fileWriter.flush();
                fileWriter.close();
                LOG.info(String.format("KafkaConsumerFileTarget putDataToFile finished processing - records processed: %d", recordCount));

            } catch (IOException e) {
                LOG.info("KafkaConsumerFileTarget IOException - cleaning up");
                e.printStackTrace();

            }
            System.out.println("KafkaConsumerFileTarget (consumer) - processing completed processing " + recordCount + " records.");
    }

    private void putDataToFile(String data){

        try {

            String streamRecord = new String();

            /* process record */
            //streamRecord = data + '\n';

            streamRecord = data;
            fileWriter.append(streamRecord);
            recordCount++;
            LOG.info("KafkaConsumerFileTarget putDataToFile processed record: " + streamRecord);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
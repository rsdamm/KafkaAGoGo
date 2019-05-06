package KafkaManagerUnitTests;


import com.plesba.kafkaworkers.source.KafkaConsumerFileTarget;
import com.plesba.kafkaworkers.target.KafkaProducerFileSource;
import com.plesba.kafkamanager.KafkaManager;
import com.plesba.kafkamanager.utils.KMProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author REnee
 */
public class KafkaConsumerFileTargetTest {
    private static String propertiesFile = "/Users/renee/IdeaProjects/KafkaAGoGO/config.properties";
    private long recordCountCSVOut = 0;
    private long recordCountCSVIn = 0;
    private long maxRecordsToProcess = 3;
    private static KafkaProducerFileSource kWriter = null;
    private static KafkaConsumerFileTarget kReader = null;
    private static final Log LOG = LogFactory.getLog(KafkaManager.class);
    private static String csvFilenameIn;
    private static String csvFilenameOut;
    private static Properties kafkaMgrProps = null;
    private static Properties kfwProp;
    private static Properties kfrProp;

    public KafkaConsumerFileTargetTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of run method, of class DBloader.
     *
     * @throws java.io.IOException
     */
    @Test
    public void testRun() throws IOException {


        LOG.info("KafkaConsumerFileTargetTest starting");

        kafkaMgrProps = new KMProperties(propertiesFile).getProp();
        LOG.info("KafkaConsumerFileTargetTest properties obtained");

        csvFilenameIn = kafkaMgrProps.getProperty("km.infilename");
        LOG.info("KafkaConsumerFileTargetTest csvFilenameIn: " + csvFilenameIn);

        csvFilenameOut = kafkaMgrProps.getProperty("km.outfilename");
        LOG.info("KafkaConsumerFileTargetTest csvFilenameOut: " + csvFilenameOut);

        //Kafka producer, read from file / write to kafka stream (producer)
        LOG.info("KafkaTargetFromStreamTest starting Kafka Target (producer). ");
        kfwProp = new Properties();
        kfwProp.setProperty("client.id", kafkaMgrProps.getProperty("kafka.client.id"));
        kfwProp.setProperty("acks", kafkaMgrProps.getProperty("kafka.acks"));
        kfwProp.setProperty("bootstrap.servers", kafkaMgrProps.getProperty("kafka.bootstrap.servers"));
        kfwProp.setProperty("topic", kafkaMgrProps.getProperty("kafka.topic"));
        kfwProp.setProperty("key.serializer", kafkaMgrProps.getProperty("kafka.key.serializer.class"));
        kfwProp.setProperty("value.serializer", kafkaMgrProps.getProperty("kafka.value.serializer.class"));
        kfwProp.setProperty("producer.type", kafkaMgrProps.getProperty("kafka.producer.type"));

        try {
            kWriter = new KafkaProducerFileSource(kfwProp, csvFilenameIn);
            new Thread(
                    new Runnable() {
                        public void run() {
                            kWriter.processData();
                        }
                    }
            ).start();
        } catch (InterruptedException ex) {
            Logger.getLogger(KafkaProducerFileSource.class.getName()).log(Level.SEVERE, null, ex);
        }

        //Kafka consumer, read from kafka stream / write to file
        LOG.info("KafkaConsumerFileTargetTest starting KafkaConsumerFileTarget (consumer). ");

        kfrProp = new Properties();
        kfrProp.setProperty("client.id", kafkaMgrProps.getProperty("kafka.client.id"));
        kfrProp.setProperty("acks", kafkaMgrProps.getProperty("kafka.acks"));
        kfrProp.setProperty("bootstrap.servers", kafkaMgrProps.getProperty("kafka.bootstrap.servers"));
        kfrProp.setProperty("topic", kafkaMgrProps.getProperty("kafka.topic"));
        kfrProp.setProperty("key.deserializer", kafkaMgrProps.getProperty("kafka.key.deserializer.class"));
        kfrProp.setProperty("value.deserializer", kafkaMgrProps.getProperty("kafka.value.deserializer.class"));
        kfrProp.setProperty("group.id", kafkaMgrProps.getProperty("kafka.group_id_config"));
        kfrProp.setProperty("maxrecordstoprocess", kafkaMgrProps.getProperty("kafka.maxrecordstoprocess"));
        //kfrProp.setProperty("maxrecordstoprocess", String.valueOf(maxRecordsToProcess));

        try {
            kReader = new KafkaConsumerFileTarget(kfrProp, csvFilenameOut);
         //   new Thread(
          //          new Runnable() {
           //             public void run() {
                            kReader.processData();
            //            }
             //       }
            //).start();
        } catch (Exception ex) {

            Logger.getLogger(KafkaConsumerFileTarget.class.getName()).log(Level.SEVERE, null, ex);
        }
        recordCountCSVIn = Files.lines(Paths.get(csvFilenameIn)).count();
        recordCountCSVOut = Files.lines(Paths.get(csvFilenameOut)).count();

        assertEquals(recordCountCSVIn, recordCountCSVOut);
        LOG.info("KafkaConsumerFileTargetTest completed");
    }

}
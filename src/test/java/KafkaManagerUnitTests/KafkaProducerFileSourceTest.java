package KafkaManagerUnitTests;


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
 *
 * @author REnee
 */
public class KafkaProducerFileSourceTest {
    private static String propertiesFile = "/Users/renee/IdeaProjects/KafkaAGoGO/config.properties";
    private long recordCountCSVIn = 0;
    private long recordCountStreamOut = 0;
    private long maxStreamCount = 0;
    private static KafkaProducerFileSource kfWriter = null;
    private static final Log LOG = LogFactory.getLog(KafkaManager.class);
    private static String csvFilenameIn;
    private static Properties kafkaMgrProps = null;
    private static Properties kfwProp;
    public KafkaProducerFileSourceTest() {
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
     * @throws java.io.IOException
     */
    @Test
    public void testRun() throws IOException {

        LOG.info("KafkaProducerFileSourceTest starting Kafka Target (producer) file as source. ");

        kafkaMgrProps = new KMProperties(propertiesFile).getProp();
        LOG.info("KafkaProducerFileSourceTest properties obtained");

        csvFilenameIn = kafkaMgrProps.getProperty("km.infilename");
        LOG.info("KafkaProducerFileSourceTest - input from " + csvFilenameIn);

        recordCountCSVIn = Files.lines(Paths.get(csvFilenameIn)).count();

        //kafka producer, read from input stream / write to kafka stream (producer)
        kfwProp = new Properties();
        kfwProp.setProperty("client.id", kafkaMgrProps.getProperty("kafka.client.id"));
        kfwProp.setProperty("acks", kafkaMgrProps.getProperty("kafka.acks"));
        kfwProp.setProperty("bootstrap.servers", kafkaMgrProps.getProperty("kafka.bootstrap.servers"));
        kfwProp.setProperty("topic", kafkaMgrProps.getProperty("kafka.topic"));
        kfwProp.setProperty("key.serializer", kafkaMgrProps.getProperty("kafka.key.serializer.class"));
        kfwProp.setProperty("value.serializer", kafkaMgrProps.getProperty("kafka.value.serializer.class"));
        kfwProp.setProperty("producer.type", kafkaMgrProps.getProperty("kafka.producer.type"));
        try {
            kfWriter = new KafkaProducerFileSource(kfwProp, csvFilenameIn);
            kfWriter.processData();
            recordCountStreamOut=kfWriter.getLoadedCount();
        } catch (InterruptedException ex) {
            Logger.getLogger(KafkaProducerFileSourceTest.class.getName()).log(Level.SEVERE, null, ex);
        }


            assertEquals(recordCountCSVIn, recordCountStreamOut);


        LOG.info("KafkaProducerFileSourceTest completed");
    }

}


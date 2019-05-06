package com.plesba.kafkamanager.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;

public class KMProperties {

    //Configuration config = null;
    String propFile;
    Properties prop;

    private static final Log LOG = LogFactory.getLog(KMProperties.class);

    public KMProperties(String pfn) {

        propFile = pfn;
        prop = new Properties();

        try {

            prop.load(new FileInputStream(propFile));

        } catch (FileNotFoundException ex) {
            LOG.info("KMProperties Error: unable to open properties");
            ex.printStackTrace();
        } catch (IOException ex) {
            LOG.info("KMProperties Error: unable to read properties file");
            ex.printStackTrace();
        }

    }
    public Properties getProp(){
        return prop;
    }


}

package com.cqyuanye.conf;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by yuanye on 2016/5/10.
 */
public class ConfigManager {

    private final String CONF_NAME = "./conf/crawler.properties";
    private final Properties PROP = new Properties();
    private final Logger LOGGER = Logger.getLogger(ConfigManager.class);

    public ConfigManager(){
        loadConf();
    }

    private void loadConf(){
        File conf = new File(CONF_NAME);
        FileInputStream in = null;

        try{
            in = new FileInputStream(conf);
            PROP.load(in);
        } catch (FileNotFoundException e) {
           LOGGER.warn("Could not found conf file ." + e.getMessage());
        } catch (IOException e) {
           LOGGER.warn("Load conf failed." + e.getMessage());
        }finally {
            if (in != null){
                try {
                    in.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
        }
    }

    public int getInt(String key,int defaultValue){
        String rawValue = PROP.getProperty(key);
        int value = defaultValue;
        if (rawValue != null){
            value = Integer.valueOf(rawValue);
        }
        return value;
    }

    public long getLong(String key,long defaultValue){
        String rawValue = PROP.getProperty(key);
        long value = defaultValue;
        if (rawValue != null){
            value = Long.valueOf(rawValue);
        }
        return value;
    }

    public float getFloat(String key,float defaultValue){
        String rawValue = PROP.getProperty(key);
        float value = defaultValue;
        if (rawValue != null){
            value = Float.valueOf(rawValue);
        }
        return value;
    }

    public boolean getBoolean(String key,boolean defaultValue){
        String rawValue = PROP.getProperty(key);
        boolean value = defaultValue;
        if (rawValue != null){
            value = Boolean.valueOf(rawValue);
        }
        return value;
    }

    public String getString(String key,String defaultValue){
        String rawValue = PROP.getProperty(key);
        return rawValue == null ?defaultValue:rawValue;
    }
}

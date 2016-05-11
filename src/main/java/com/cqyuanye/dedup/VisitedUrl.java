package com.cqyuanye.dedup;

import com.cqyuanye.utils.MD5;
import com.sleepycat.je.*;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

/**
 * Created by yuanye on 2016/5/9.
 */
public class VisitedUrl {
    //数据库环境
    private Environment myDbEnvironment = null;
    private DatabaseConfig dbConfig = null;
    private Database myDatabase = null;
    private final String fileName = "E:\\data\\crawl\\db\\berkelyDB";
    private final String dbName = "url";

    private static final VisitedUrl instance = new VisitedUrl();

    private VisitedUrl(){
    }

    @Deprecated
    private static VisitedUrl getInstance(){
        return instance;
    }

    public void openDB() {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setReadOnly(false);
        envConfig.setTxnTimeout(10000, TimeUnit.MILLISECONDS);

        File file = new File(fileName);
        if (!file.exists())
            file.mkdirs();
        myDbEnvironment = new Environment(file, envConfig);

        dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        dbConfig.setReadOnly(false);

        if (myDatabase == null)
            myDatabase = myDbEnvironment.openDatabase(null, dbName, dbConfig);
    }

    public boolean exists(String url) {
        boolean result = false;
        try {
            String md5 = MD5.toMd5(url.toLowerCase());
            DatabaseEntry theKey = new DatabaseEntry(md5.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry();
            Transaction txn = null;
            try {
                TransactionConfig txConfig = new TransactionConfig();
                txConfig.setSerializableIsolation(true);
                txn = myDbEnvironment.beginTransaction(null, txConfig);
                OperationStatus res = myDatabase.get(txn, theKey, theData, LockMode.DEFAULT);
                txn.commit();
                if (res == OperationStatus.SUCCESS) {
                    byte[] retData = theData.getData();
                    String foundData = new String(retData, "UTF-8");
                    result = true;
                }
            } catch (LockConflictException lockConflict) {
                txn.abort();
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public boolean addUrl(String url) {

        boolean result = false;
        try {
            //设置key/value,注意DatabaseEntry内使用的是bytes数组
            String md5 = MD5.toMd5(url.toLowerCase());
            DatabaseEntry theKey = new DatabaseEntry(md5.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry("true".getBytes("UTF-8"));
            OperationStatus res = null;
            Transaction txn = null;
            try {
                TransactionConfig txConfig = new TransactionConfig();
                txConfig.setSerializableIsolation(true);
                txn = myDbEnvironment.beginTransaction(null, txConfig);
                res = myDatabase.put(txn, theKey, theData);
                txn.commit();
                if (res == OperationStatus.SUCCESS) {
                    result = true;
                }
            } catch (LockConflictException lockConflict) {
                assert txn != null;
                txn.abort();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public void close() {
        if (myDatabase != null) {
            myDatabase.close();
        }
        if (myDbEnvironment != null) {
            myDbEnvironment.cleanLog();
            myDbEnvironment.close();
        }
    }
}
package com.cqyuanye.dedup;

import com.codahale.metrics.Counter;
import com.cqyuanye.CrawlerContext;
import com.cqyuanye.Service;
import com.cqyuanye.conf.ConfigManager;
import com.cqyuanye.metrics.CrawlerMetrics;
import com.cqyuanye.utils.MD5;
import com.sleepycat.je.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

/**
 * Created by yuanye on 2016/5/10.
 * Using Berkeley DB to store visited url,and check whether new url visited.
 */
public class URLDedupService implements Service {

    private ConfigManager configManager;
    private CrawlerMetrics metrics;
    private String dbDir;
    private String dbName;

    private Environment myDbEnvironment = null;
    private DatabaseConfig dbConfig = null;
    private Database myDatabase = null;

    private Counter newUrlCounter;
    private Counter checkCounter;

    private final Logger LOGGER = Logger.getLogger(URLDedupService.class);

    @Override
    public void init(CrawlerContext context) throws Exception {
        this.configManager = context.configManager;
        this.metrics = context.metrics;
        doInit();
    }

    @Override
    public void start() {
        openDB();
    }

    @Override
    public void shutdown() {
        closeDB();
    }

    public void visit(String url) {
        newUrlCounter.inc();
        String md5 = MD5.toMd5(url.trim().toLowerCase());
        try {
            DatabaseEntry theKey = new DatabaseEntry(md5.getBytes("UTF-8"));
            DatabaseEntry theData = new DatabaseEntry("true".getBytes("UTF-8"));
            OperationStatus res;
            Transaction txn = null;
            try {
                TransactionConfig txConfig = new TransactionConfig();
                txConfig.setSerializableIsolation(true);
                txn = myDbEnvironment.beginTransaction(null, txConfig);
                res = myDatabase.put(txn, theKey, theData);
                txn.commit();
                if (res != OperationStatus.SUCCESS) {
                    LOGGER.warn("Insert url " + url + " Berkeley DB failed." + res.toString());
                }
            } catch (LockConflictException lockConflict) {
                assert txn != null;
                txn.abort();
                LOGGER.warn("Insert url " + url + " Berkeley DB failed. Abort the transaction");
            }
        } catch (UnsupportedEncodingException e) {
            //would not happen.
            LOGGER.error(e.getMessage());
        }
    }

    public boolean visited(String url) {
        checkCounter.inc();
        boolean result = false;
        try {
            String md5 = MD5.toMd5(url.trim().toLowerCase());
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
                    if (foundData.equals("true")) {
                        result = true;
                    }
                }
            } catch (LockConflictException lockConflict) {
                assert txn != null;
                txn.abort();
            }
        } catch (UnsupportedEncodingException e) {
            // wouldn't happen.
        }
        return result;
    }

    private void doInit() throws Exception {
        dbDir = configManager.getString("url.db.dir", "");
        dbName = configManager.getString("url.db.name", "url");

        if (dbDir.trim().length() == 0) {
            throw new Exception("url.db.dir must be setted.");
        }

        newUrlCounter = metrics.newCounter(URLDedupService.class,"url.visited","count");
        checkCounter = metrics.newCounter(URLDedupService.class,"url.check","count");
    }

    private void openDB() {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setReadOnly(false);
        envConfig.setTxnTimeout(10000, TimeUnit.MILLISECONDS);

        File file = new File(dbDir);
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

    private void closeDB() {
        if (myDatabase != null) {
            myDatabase.close();
        }
        if (myDbEnvironment != null) {
            myDbEnvironment.cleanLog();
            myDbEnvironment.close();
        }
    }
}

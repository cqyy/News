package com.cqyuanye.crawler.fs;

import com.codahale.metrics.Counter;
import com.cqyuanye.CrawlerContext;
import com.cqyuanye.News;
import com.cqyuanye.Service;
import com.cqyuanye.conf.ConfigManager;
import com.cqyuanye.metrics.CrawlerMetrics;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by yuanye on 2016/5/5.
 */
public class FSService implements Service {

    private final BlockingQueue<News> newsQueue = new LinkedBlockingQueue<>();
    private final WriterThread writer = new WriterThread();
    private final Logger LOGGER = Logger.getLogger(FSService.class);
    private final String INVALID_FILENAME_CHAR = "[\\/\\:\\*\\?\"<>|]";

    private String root;
    private ConfigManager configManager;
    private CrawlerMetrics metrics;
    private Counter newsCounter;
    private Counter newsSizeCounter;

    public FSService() {
    }

    public void save(News news) {
        try {
            newsQueue.put(news);
        } catch (InterruptedException e) {
            //do nothing
        }
    }

    @Override
    public void init(CrawlerContext context) {
        this.configManager = context.configManager;
        this.metrics = context.metrics;
        root = this.configManager.getString("fs.data.dir","./data/crawl/news");
        initMetrics();
    }

    private void initMetrics(){
        metrics.newGauge(newsQueue::size,FSService.class,"news.queue","size");
        newsCounter = metrics.newCounter(FSService.class,"news.write","count");
        newsSizeCounter = metrics.newCounter(FSService.class,"news.write","size");
    }

    @Override
    public void start() {
        writer.start();
    }

    @Override
    public void shutdown() {
        writer.interrupt();
    }

    private class WriterThread extends Thread {
        @Override
        public void run() {
            FileOutputStream os = null;
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat ISO8601_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.CHINA);

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    News news = newsQueue.poll(5, TimeUnit.SECONDS);
                    if (news != null) {
                        String source = news.source;
                        String dateStr = dateFormat.format(news.date);
                        String title = news.title;

                        String path = source + "/" + dateStr + "/"
                                + title.replaceAll(INVALID_FILENAME_CHAR,"-") + ".json";

                        File file = new File(root, path);
                        if (!file.exists()) {
                            try {
                                if ( !(file.getParentFile().exists() || file.getParentFile().mkdirs())
                                        || !file.createNewFile()) {
                                    throw new IOException("Create file failed." + file);
                                }
                            } catch (IOException e) {
                                LOGGER.error("Create new file failed." + e.getMessage());
                            }
                        }

                        JSONObject json = new JSONObject();
                        json.put("title",news.title)
                                .put("date",ISO8601_DATE_FORMAT.format(news.date))
                                .put("source",news.source)
                                .put("url", news.url)
                                .put("content",news.content);

                        os = new FileOutputStream(file);
                        os.write(json.toString().getBytes());
                        newsCounter.inc();
                        newsSizeCounter.inc(json.toString().length());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }catch (Throwable throwable){
                    LOGGER.warn(throwable.getMessage());
                }finally {
                    if (os != null) {
                        try {
                            os.close();
                        } catch (IOException e) {
                            //do nothing
                        }
                    }
                }
            }
        }
    }
}

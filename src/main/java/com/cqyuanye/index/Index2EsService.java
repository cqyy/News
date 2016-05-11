package com.cqyuanye.index;

import com.codahale.metrics.Counter;
import com.cqyuanye.CrawlerContext;
import com.cqyuanye.News;
import com.cqyuanye.Service;
import com.cqyuanye.conf.ConfigManager;
import com.cqyuanye.metrics.CrawlerMetrics;
import com.cqyuanye.utils.MD5;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by yuanye on 2016/5/11.
 */
public class Index2EsService implements Service {

    private final BlockingQueue<News> JSON_QUEUE = new LinkedBlockingQueue<>();
    private final IndexThread indexThread = new IndexThread();
    private final Logger LOGGER = Logger.getLogger(Index2EsService.class);

    private int indexBulkSize;
    private String clusterName;
    private String indexName;
    private String indexType;
    //host and port of elasticSearch cluster.Ex.192.168.99.1:9200.
    private List<String> clusterAddress;

    private ConfigManager configManager;
    private CrawlerMetrics metrics;
    private Counter indexCounter;

    private volatile boolean shutdown = false;

    @Override
    public void init(CrawlerContext context) throws Exception {
        this.configManager = context.configManager;
        this.metrics = context.metrics;
        indexBulkSize = configManager.getInt("index.bulk.size", 50);
        clusterName = configManager.getString("index.cluster.name", "news");
        indexName = configManager.getString("index.index.name","news");
        indexType = configManager.getString("index.index.type","rmrb");

        String rawAddress = configManager.getString("index.cluster.addr", "localhost:9300");
        String[] addrs = rawAddress.split(",");
        clusterAddress = Arrays.asList(addrs);

        metrics.newGauge(JSON_QUEUE::size,Index2EsService.class,"index.queue","size");
        indexCounter = metrics.newCounter(Index2EsService.class,"index.indexed","count");
    }

    @Override
    public void start() {
        indexThread.start();
    }

    @Override
    public void shutdown() {
        shutdown = true;
        while (!JSON_QUEUE.isEmpty()) {
            LOGGER.info("waiting for remain data to index. Remaining data size: " + JSON_QUEUE.size());
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                //do nothing
            }
        }
        indexThread.interrupt();
    }

    public void index(News news){
        if (!shutdown){
            while (!JSON_QUEUE.offer(news));
        }
    }

    private class IndexThread extends Thread {
        @Override
        public void run() {
            Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();
            TransportClient client;
            client = TransportClient.builder().settings(settings).build();
            for (String addr : clusterAddress) {
                String[] values = addr.split(":");
                if (values.length == 2) {
                    String host = values[0];
                    int port = Integer.valueOf(values[1]);
                    try {
                        client.addTransportAddress(
                                new InetSocketTransportAddress(InetAddress.getByName(host), port));
                    } catch (UnknownHostException e) {
                        LOGGER.error("Unknown host of ES cluster." + e.getMessage());
                    }
                } else {
                    LOGGER.error("Invalid host:port pair. " + addr);
                }
            }

            BulkRequestBuilder bulkRequest = client.prepareBulk();
            int count = 0;

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    News news = JSON_QUEUE.poll(5, TimeUnit.SECONDS);
                    if (news != null) {
                        String url = news.url;
                        //using the md5 of url as the news id.
                        String id = MD5.toMd5(url.trim());
                        bulkRequest.add(client.prepareIndex(indexName, indexType, id)
                                .setSource(
                                        jsonBuilder()
                                                .startObject()
                                                .field("title",news.title)
                                                .field("url",news.url)
                                                .field("content",news.content)
                                                .field("date",news.date)
                                                .field("source",news.source).endObject()));

                        if ((++count % indexBulkSize) == 0) {
                            BulkResponse responses = bulkRequest.execute().actionGet();
                            if (responses.hasFailures()) {
                                LOGGER.warn(responses.buildFailureMessage());
                            }
                            bulkRequest = client.prepareBulk();
                        }
                        indexCounter.inc();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            client.close();
        }
    }
}

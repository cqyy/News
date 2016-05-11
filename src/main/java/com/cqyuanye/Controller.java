package com.cqyuanye;

import com.cqyuanye.conf.ConfigManager;
import com.cqyuanye.cralwer.news.people.PeopleCrawler;
import com.cqyuanye.crawler.fs.FSService;
import com.cqyuanye.dedup.URLDedupService;
import com.cqyuanye.index.Index2EsService;
import com.cqyuanye.metrics.CrawlerMetrics;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Controller {
    public static void main(String[] args) throws Exception {
        String crawlStorageFolder = "./crawl/root";
        int numberOfCrawlers = 7;

        CrawlConfig config = new CrawlConfig();
        config.setCrawlStorageFolder(crawlStorageFolder);
        config.setMaxDepthOfCrawling(4);

        /*
         * Instantiate the controller for this crawl.
         */
        PageFetcher pageFetcher = new PageFetcher(config);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);
        /*
         * For each crawl, you need to add some seed urls. These are the first
         * URLs that are fetched and then the crawler starts following links
         * which are found in these pages
         */
        controller.addSeed("http://www.people.com.cn/");

        List<Service> services = new ArrayList<>(3);
        FSService fsService = new FSService();
        Index2EsService indexService = new Index2EsService();
        URLDedupService dedupService = new URLDedupService();

        services.add(fsService);
        services.add(indexService);
        services.add(dedupService);

        ConfigManager configManager = new ConfigManager();
        CrawlerMetrics metrics = new CrawlerMetrics();

        CrawlerContext crawlerContext = new CrawlerContext(configManager,fsService,
                dedupService,indexService, metrics);

        for (Service service : services){
            service.init(crawlerContext);
            service.start();
        }
        /*
         * Start the crawl. This is a blocking operation, meaning that your code
         * will reach the line after this only when crawling is finished.
         */
        controller.setCustomData(crawlerContext);
        controller.startNonBlocking(PeopleCrawler.class, numberOfCrawlers);

        metrics.report2console(20, TimeUnit.SECONDS);
        metrics.report2Html("/var/www/html/index.html",20,TimeUnit.SECONDS);

        controller.waitUntilFinish();

        for (Service service : services){
            service.shutdown();
        }
    }
}
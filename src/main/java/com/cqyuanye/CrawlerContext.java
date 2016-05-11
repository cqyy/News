package com.cqyuanye;

import com.cqyuanye.conf.ConfigManager;
import com.cqyuanye.crawler.fs.FSService;
import com.cqyuanye.dedup.URLDedupService;
import com.cqyuanye.index.Index2EsService;
import com.cqyuanye.metrics.CrawlerMetrics;

/**
 * Created by yuanye on 2016/5/10.
 */
public class CrawlerContext {

    public final ConfigManager configManager;
    public final FSService fsService;
    public final URLDedupService dedupService;
    public final Index2EsService indexService;
    public final CrawlerMetrics metrics;


    public CrawlerContext(ConfigManager configManager, FSService fsService,
                          URLDedupService dedupService, Index2EsService indexService,
                          CrawlerMetrics metrics) {
        this.configManager = configManager;
        this.fsService = fsService;
        this.dedupService = dedupService;
        this.indexService = indexService;
        this.metrics = metrics;
    }
}

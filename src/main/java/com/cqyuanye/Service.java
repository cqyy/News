package com.cqyuanye;

/**
 * Created by yuanye on 2016/5/10.
 */
public interface Service {

    void init(CrawlerContext context) throws Exception;

    void start();

    void shutdown();
}

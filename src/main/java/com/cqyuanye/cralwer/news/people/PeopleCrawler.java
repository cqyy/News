package com.cqyuanye.cralwer.news.people;

import com.cqyuanye.CrawlerContext;
import com.cqyuanye.News;
import com.cqyuanye.crawler.fs.FSService;
import com.cqyuanye.dedup.URLDedupService;
import com.cqyuanye.index.Index2EsService;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Created by yuanye on 2016/5/6.
 * 人民网新闻爬虫
 */
public class PeopleCrawler extends WebCrawler {

    private static final Pattern FILTER = Pattern.compile("^http:\\/\\/\\w+.people.com.cn\\/(.*.html)?$");
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy年MM月dd日HH:mm");
    private static final Logger LOGGER = Logger.getLogger(PeopleCrawler.class);

    private CrawlerContext context;
    private URLDedupService dedupService;
    private FSService fsService;
    private Index2EsService indexService;

    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        boolean shouldVisit = false;

        boolean matched = FILTER.matcher(url.getURL()).matches();
        if (matched){
            if (url.getURL().endsWith(".html") && !dedupService.visited(url.getURL())){
                shouldVisit = true;
            }
        }
        return shouldVisit;
    }

    @Override
    public void init(int id, CrawlController crawlController) {
        super.init(id, crawlController);
        context = (CrawlerContext) crawlController.getCustomData();
        dedupService = context.dedupService;
        fsService = context.fsService;
        indexService = context.indexService;
    }

    @Override
    public void visit(Page page) {
        if (page.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            String html = htmlParseData.getHtml();
            Document doc = Jsoup.parse(html);
            Element dateE = doc.select("#p_publishtime").first();
            Element contentE = doc.select("#p_content").first();

            if (dateE != null && contentE != null) {
                try {
                    String title = htmlParseData.getTitle();
                    String url = page.getWebURL().getURL();
                    String source = "人民网";
                    String content = contentE.text();
                    Date date = DATE_FORMAT.parse(dateE.text());
                    News news = new News(title, source, content, date, url);

                    fsService.save(news);
                    indexService.index(news);
                    dedupService.visit(url);
                } catch (ParseException e) {
                    LOGGER.warn(e);
                }
            }else {
                LOGGER.info("Not content page. URL: " + page.getWebURL().getURL());
            }
        }
    }
}

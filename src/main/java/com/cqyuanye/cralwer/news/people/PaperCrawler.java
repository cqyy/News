package com.cqyuanye.cralwer.news.people;

import com.cqyuanye.News;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yuanye on 2016/5/5.
 * 人民日报爬虫
 */
public class PaperCrawler extends WebCrawler {

    private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|gif|jpg"
            + "|png|mp3|mp3|zip|gz|pdf))$");

    private final static Pattern DATE_MATCHER = Pattern.compile("(\\d{4}年\\d{2}月\\d{2}日)");
    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy年MM月dd日");


    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        String href = url.getURL().toLowerCase();
        return !FILTERS.matcher(href).matches()
                && href.startsWith("http://paper.people.com.cn/");
    }

    @Override
    public void visit(Page page) {
        if (page.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            String html = htmlParseData.getHtml();
            String title = htmlParseData.getTitle();
            String url = page.getWebURL().getURL();
            String source = "人民日报";
            String content;
            Date date;
            Document doc = Jsoup.parse(html);

            Element dateE = doc.select(".lai").first();
            Element contentE = doc.select(".c_c").first();

            if (dateE == null || contentE == null) {
                System.out.println("Parse failed ,URL: " + page.getWebURL().getURL());
                return;
            }
            Matcher matcher = DATE_MATCHER.matcher(dateE.text());
            if (matcher.find()) {
                String dateStr = matcher.group(1);
                try {
                    date = DATE_FORMAT.parse(dateStr);
                } catch (ParseException e) {
                    e.printStackTrace();
                    return;
                }
            }else{
                System.out.println("Could not found date");
                return;
            }

            content = contentE.text();

            News news = new News(title, source, content, date, url);
            //FileSystem.getInstance().save(news);
        }
    }
}

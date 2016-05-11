package com.cqyuanye;

import java.util.Date;

/**
 * Created by yuanye on 2016/5/5.
 */
public class News {

    public final String title;
    public final String source;
    public final String content;
    public final Date date;
    public final String url;

    public News(String title, String source, String content, Date date, String url) {
        this.title = title;
        this.source = source;
        this.content = content;
        this.date = date;
        this.url = url;
    }
}

package com.cqyuanye.metrics;


import com.codahale.metrics.*;
import org.pegdown.PegDownProcessor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Created by yuanye on 2016/5/11.
 */
public class CrawlerMetrics {

    private static final String P1 = "# ";
    private static final String P2 = "## ";
    private static final String P3 = "### ";

    private static final String newLine = "\n";


    private final MetricRegistry metrics = new MetricRegistry();

    public void newGauge(Gauge<Integer> gauge, Class<?> clazz, String... names) {
        metrics.register(MetricRegistry.name(clazz, names), gauge);
    }

    public Counter newCounter(Class<?> clazz, String... names) {
        return metrics.counter(MetricRegistry.name(clazz, names));
    }

    public Histogram newHistogram(Class<?> clazz, String... names) {
        return metrics.histogram(MetricRegistry.name(clazz, names));
    }

    public Timer newTimer(Class<?> clazz, String... names) {
        return metrics.timer(MetricRegistry.name(clazz, names));
    }

    public void report2console(int interval, TimeUnit timeunit) {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(interval, timeunit);
    }

    public void report2Html (String path,int interval,TimeUnit timeUnit) throws  IOException{
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(() -> {
            try{
                generateHtml(path);
            }catch (IOException e){
                e.printStackTrace();
            }
        }, interval, interval, timeUnit);
    }

    private void generateHtml(String path) throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append("**").append(new Date()).append("**");

        //Gauges
        sb.append(newLine).append(P1)
                .append("Gauges").append(newLine);
        for (Map.Entry<String, Gauge> gauge : metrics.getGauges().entrySet()) {
            String key = gauge.getKey();
            String value = gauge.getValue().getValue().toString();

            sb.append(newLine).append("- ").append(key).append("  ").append(newLine)
                    .append("> ").append("value = ").append(value);
        }

        //Counters
        sb.append(newLine).append(newLine).append(P1).append("Counters").append(newLine);
        for (Map.Entry<String, Counter> counter : metrics.getCounters().entrySet()) {
            String key = counter.getKey();
            long count = counter.getValue().getCount();

            sb.append(newLine).append("- ").append(key).append("  ").append(newLine)
                    .append("> ").append("value = ").append(count);
        }

        String markdown = sb.toString();
        PegDownProcessor processor = new PegDownProcessor();
        String html = processor.markdownToHtml(markdown);

        File file = new File(path);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        FileOutputStream os = null;
        IOException ex = null;
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            os = new FileOutputStream(file);
            os.write(html.getBytes("UTF-8"));
        } catch (IOException e) {
            ex = e;
        } finally {
            try {
                if (os != null) {
                    os.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (ex != null){
            throw ex;
        }
    }

}

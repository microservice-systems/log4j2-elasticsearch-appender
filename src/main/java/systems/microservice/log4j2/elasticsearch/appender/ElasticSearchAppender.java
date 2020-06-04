/*
 * Copyright (C) 2020 Dmitry Kotlyarov.
 * All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package systems.microservice.log4j2.elasticsearch.appender;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
@Plugin(name = "ElasticSearch", category = "Core", elementType = "appender", printObject = true)
public final class ElasticSearchAppender extends AbstractAppender {
    public static final long PROCESS_ID = createProcessID();
    public static final long PROCESS_START_TIME = createProcessStartTime();
    public static final Map<String, String> LOG_TAGS = createLogTags();
    public static final String HOST_NAME = createHostName();
    public static final String HOST_IP = createHostIP();
    public static final String ENVIRONMENT_VARIABLES = createEnvironmentVariables();
    public static final String SYSTEM_PROPERTIES = createSystemProperties();

    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean flag = new AtomicBoolean(true);
    private final AtomicLong lost = new AtomicLong(0L);
    private final AtomicLong lostSinceTime = new AtomicLong(System.currentTimeMillis());
    private final String url;
    private final String index;
    private final int countMax;
    private final long sizeMax;
    private final int lengthMax;
    private final long spanMax;
    private final RestHighLevelClient client;
    private final Buffer buffer1;
    private final Buffer buffer2;
    private final Thread flushThread;

    public ElasticSearchAppender(String name,
                                 String url,
                                 String index,
                                 int countMax,
                                 long sizeMax,
                                 int lengthMax,
                                 long spanMax,
                                 Filter filter,
                                 Layout<? extends Serializable> layout) {
        super(name, filter, (layout != null) ? layout : PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);

        if ((url != null) && (index != null)) {
            this.url = url;
            this.index = index;
            this.countMax = countMax;
            this.sizeMax = sizeMax;
            this.lengthMax = lengthMax;
            this.spanMax = spanMax * 1000L;
            this.client = createClient(url);
            this.buffer1 = new Buffer(countMax, sizeMax);
            this.buffer2 = new Buffer(countMax, sizeMax);
            this.flushThread = new Thread(String.format("log4j2-elasticsearch-appender-flush-%s", name)) {
                @Override
                public void run() {
                    Runtime.getRuntime().addShutdownHook(new Thread(String.format("log4j2-elasticsearch-appender-shutdown-%s", name)) {
                        @Override
                        public void run() {
                            final AtomicBoolean enabled = ElasticSearchAppender.this.enabled;
                            final Thread flushThread = ElasticSearchAppender.this.flushThread;
                            enabled.set(false);
                            flushThread.interrupt();
                            try {
                                flushThread.join();
                            } catch (InterruptedException e) {
                            }
                        }
                    });
                    final AtomicBoolean enabled = ElasticSearchAppender.this.enabled;
                    final AtomicBoolean flag = ElasticSearchAppender.this.flag;
                    final AtomicLong lost = ElasticSearchAppender.this.lost;
                    final AtomicLong lostSince = ElasticSearchAppender.this.lostSinceTime;
                    final String url = ElasticSearchAppender.this.url;
                    final String index = ElasticSearchAppender.this.index;
                    final long spanMax = ElasticSearchAppender.this.spanMax;
                    final RestHighLevelClient client = ElasticSearchAppender.this.client;
                    final Buffer buffer1 = ElasticSearchAppender.this.buffer1;
                    final Buffer buffer2 = ElasticSearchAppender.this.buffer2;
                    long pt = System.currentTimeMillis();
                    while (enabled.get()) {
                        long t = System.currentTimeMillis();
                        if (t >= pt + spanMax) {
                            if (flag.get()) {
                                try {
                                    flag.set(false);
                                    buffer1.flush(client, url, index, lost, lostSince);
                                } catch (Throwable e) {
                                    ElasticSearchAppender.logSystem(ElasticSearchAppender.class, e.getMessage());
                                }
                            } else {
                                try {
                                    flag.set(true);
                                    buffer2.flush(client, url, index, lost, lostSince);
                                } catch (Throwable e) {
                                    ElasticSearchAppender.logSystem(ElasticSearchAppender.class, e.getMessage());
                                }
                            }
                            pt = t;
                        }
                        if (!buffer1.isReady()) {
                            try {
                                flag.set(false);
                                buffer1.flush(client, url, index, lost, lostSince);
                            } catch (Throwable e) {
                                ElasticSearchAppender.logSystem(ElasticSearchAppender.class, e.getMessage());
                            }
                        }
                        if (!buffer2.isReady()) {
                            try {
                                flag.set(true);
                                buffer2.flush(client, url, index, lost, lostSince);
                            } catch (Throwable e) {
                                ElasticSearchAppender.logSystem(ElasticSearchAppender.class, e.getMessage());
                            }
                        }
                        try {
                            Thread.sleep(1000L);
                        } catch (InterruptedException e) {
                        }
                    }
                    try {
                        append(new InputLogEvent(false));
                    } catch (Throwable e) {
                        ElasticSearchAppender.logSystem(ElasticSearchAppender.class, e.getMessage());
                    }
                    try {
                        flag.set(false);
                        buffer1.flush(client, url, index, lost, lostSince);
                    } catch (Throwable e) {
                        ElasticSearchAppender.logSystem(ElasticSearchAppender.class, e.getMessage());
                    }
                    try {
                        flag.set(true);
                        buffer2.flush(client, url, index, lost, lostSince);
                    } catch (Throwable e) {
                        ElasticSearchAppender.logSystem(ElasticSearchAppender.class, e.getMessage());
                    }
                }
            };
            append(new InputLogEvent(true));
        } else {
            this.url = url;
            this.index = null;
            this.countMax = 0;
            this.sizeMax = 0L;
            this.lengthMax = 0;
            this.spanMax = 0L;
            this.client = null;
            this.buffer1 = null;
            this.buffer2 = null;
            this.flushThread = null;
        }
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public String getIndex() {
        return index;
    }

    public int getCountMax() {
        return countMax;
    }

    public long getSizeMax() {
        return sizeMax;
    }

    public int getLengthMax() {
        return lengthMax;
    }

    public long getSpanMax() {
        return spanMax;
    }

    @Override
    public void start() {
        super.start();
        if ((url != null) && (index != null)) {
            enabled.set(true);
            flushThread.setDaemon(false);
            flushThread.start();
        }
    }

    @Override
    public void stop() {
        super.stop();
        if ((url != null) && (index != null)) {
            enabled.set(false);
            flushThread.interrupt();
            try {
                flushThread.join();
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void append(LogEvent event) {
        if (enabled.get()) {
            append(new InputLogEvent(event, lengthMax));
        }
    }

    private void append(InputLogEvent event) {
        if (flag.get()) {
            if (!buffer1.append(event)) {
                if (!buffer2.append(event)) {
                    lost.incrementAndGet();
                }
            }
        } else {
            if (!buffer2.append(event)) {
                if (!buffer1.append(event)) {
                    lost.incrementAndGet();
                }
            }
        }
    }

    @PluginFactory
    public static ElasticSearchAppender createAppender(@PluginAttribute("name") String name,
                                                       @PluginAttribute("url") String url,
                                                       @PluginAttribute("index") String index,
                                                       @PluginAttribute("countMax") String countMax,
                                                       @PluginAttribute("sizeMax") String sizeMax,
                                                       @PluginAttribute("lengthMax") String lengthMax,
                                                       @PluginAttribute("spanMax") String spanMax,
                                                       @PluginElement("Filter") Filter filter,
                                                       @PluginElement("Layout") Layout<? extends Serializable> layout) {
        return new ElasticSearchAppender((name != null) ? name : "elasticsearch",
                                         getProperty("log4j2.elasticsearch.url", "LOG4J2_ELASTICSEARCH_URL", url, null),
                                         getProperty("log4j2.elasticsearch.index", "LOG4J2_ELASTICSEARCH_INDEX", index, null),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.count.max", "LOG4J2_ELASTICSEARCH_COUNT_MAX", countMax, "10000")),
                                         Long.parseLong(getProperty("log4j2.elasticsearch.size.max", "LOG4J2_ELASTICSEARCH_SIZE_MAX", sizeMax, "5242880")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.length.max", "LOG4J2_ELASTICSEARCH_LENGTH_MAX", lengthMax, "4096")),
                                         Long.parseLong(getProperty("log4j2.elasticsearch.span.max", "LOG4J2_ELASTICSEARCH_SPAN_MAX", spanMax, "60")),
                                         filter,
                                         layout);
    }

    public static void logSystem(Class clazz, String message) {
        if (clazz == null) {
            clazz = ElasticSearchAppender.class;
        }
        if (message == null) {
            message = "null";
        }
        String t = String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL", System.currentTimeMillis());
        System.out.println(String.format("%s [%s] [%s] SYSTEM - %s", t, Thread.currentThread().getName(), clazz.getSimpleName(), message));
    }

    private static long createProcessID() {
        String n = ManagementFactory.getRuntimeMXBean().getName();
        if (n != null) {
            String[] ns = n.split("@");
            if (ns.length > 0) {
                try {
                    return Long.parseLong(ns[0]);
                } catch (Exception e) {
                }
            }
        }
        return -1L;
    }

    private static long createProcessStartTime() {
        return ManagementFactory.getRuntimeMXBean().getStartTime();
    }

    private static Map<String, String> createLogTags() {
        final String PREFIX_EV = "LOGTAG_";
        final String PREFIX_SP = "logtag.";
        Map<String, String> evs = System.getenv();
        Properties sps = System.getProperties();
        LinkedHashMap<String, String> lts = new LinkedHashMap<>(evs.size() + sps.size());
        for (Map.Entry<String, String> e : evs.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            if ((k != null) && (v != null)) {
                k = k.trim();
                if ((k.length() > PREFIX_EV.length()) && k.startsWith(PREFIX_EV)) {
                    lts.put(k.substring(PREFIX_EV.length()).replace('_', '.').toLowerCase(), v);
                }
            }
        }
        for (Map.Entry<Object, Object> e : sps.entrySet()) {
            Object k = e.getKey();
            Object v = e.getValue();
            if ((k != null) && (v != null)) {
                if ((k instanceof String) && (v instanceof String)) {
                    String ks = ((String) k).trim();
                    String vs = (String) v;
                    if ((ks.length() > PREFIX_SP.length()) && ks.startsWith(PREFIX_SP)) {
                        lts.put(ks.substring(PREFIX_SP.length()).replace('_', '.').toLowerCase(), vs);
                    }
                }
            }
        }
        return Collections.unmodifiableMap(lts);
    }

    private static String createHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    private static String createHostIP() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    private static String createEnvironmentVariables() {
        Map<String, String> evs = System.getenv();
        StringBuilder sb = new StringBuilder(8192);
        for (Map.Entry<String, String> e : evs.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            if ((k != null) && (v != null)) {
                sb.append(k);
                sb.append("=");
                sb.append(v);
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    private static String createSystemProperties() {
        Properties sps = System.getProperties();
        StringBuilder sb = new StringBuilder(8192);
        for (Map.Entry<Object, Object> e : sps.entrySet()) {
            Object k = e.getKey();
            Object v = e.getValue();
            if ((k != null) && (v != null)) {
                if ((k instanceof String) && (v instanceof String)) {
                    String ks = (String) k;
                    String vs = (String) v;
                    sb.append(ks);
                    sb.append("=");
                    sb.append(vs);
                    sb.append("\n");
                }
            }
        }
        return sb.toString();
    }

    private static String getProperty(String property, String variable, String value) {
        String v = getProperty(property, variable, value, null);
        if (v != null) {
            return v;
        } else {
            throw new RuntimeException(String.format("Property ['%s', '%s'] is not defined", property, variable));
        }
    }

    private static String getProperty(String property, String variable, String value, String def) {
        String v = System.getProperty(property);
        if (v != null) {
            return v;
        } else {
            v = System.getenv(variable);
            if (v != null) {
                return v;
            } else {
                if (value != null) {
                    return value;
                } else {
                    return def;
                }
            }
        }
    }

    private static RestHighLevelClient createClient(String url) {
        try {
            URL u = new URL(url);
            return new RestHighLevelClient(RestClient.builder(new HttpHost(u.getHost(), u.getPort(), u.getProtocol())));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }
}

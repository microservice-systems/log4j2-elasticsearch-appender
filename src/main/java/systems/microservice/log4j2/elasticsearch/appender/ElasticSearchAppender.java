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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
@Plugin(name = "ElasticSearch", category = "Core", elementType = "appender", printObject = true)
public final class ElasticSearchAppender extends AbstractAppender {
    public static final long PROCESS_ID = createProcessID();
    public static final UUID PROCESS_UUID = createProcessUUID();
    public static final long PROCESS_START_TIME = createProcessStartTime();
    public static final Map<String, String> LOG_TAGS = createLogTags();
    public static final String HOST_NAME = createHostName();
    public static final String HOST_IP = createHostIP();

    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean flag = new AtomicBoolean(true);
    private final AtomicLong totalCount = new AtomicLong(0L);
    private final AtomicLong totalSize = new AtomicLong(0L);
    private final AtomicLong lostCount = new AtomicLong(0L);
    private final AtomicLong lostSize = new AtomicLong(0L);
    private final String url;
    private final String index;
    private final boolean enable;
    private final int countMax;
    private final long sizeMax;
    private final int bulkCountMax;
    private final long bulkSizeMax;
    private final long delayMax;
    private final int bulkRetryCount;
    private final long bulkRetryDelay;
    private final int lengthStringMax;
    private final boolean out;
    private final boolean setDefaultUncaughtExceptionHandler;
    private final RestHighLevelClient client;
    private final Buffer buffer1;
    private final Buffer buffer2;
    private final Thread flushThread;

    public ElasticSearchAppender(String name,
                                 String url,
                                 String index,
                                 boolean enable,
                                 int countMax,
                                 long sizeMax,
                                 int bulkCountMax,
                                 long bulkSizeMax,
                                 long delayMax,
                                 int bulkRetryCount,
                                 long bulkRetryDelay,
                                 int lengthStringMax,
                                 boolean out,
                                 boolean setDefaultUncaughtExceptionHandler,
                                 Filter filter,
                                 Layout<? extends Serializable> layout) {
        super(name, filter, (layout != null) ? layout : PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);

        this.url = url;
        this.index = index;
        this.enable = enable;
        this.countMax = countMax;
        this.sizeMax = sizeMax;
        this.bulkCountMax = bulkCountMax;
        this.bulkSizeMax = bulkSizeMax;
        this.delayMax = delayMax * 1000L;
        this.bulkRetryCount = bulkRetryCount;
        this.bulkRetryDelay = bulkRetryDelay * 1000L;
        this.lengthStringMax = lengthStringMax;
        this.out = out;
        this.setDefaultUncaughtExceptionHandler = setDefaultUncaughtExceptionHandler;

        if ((url != null) && (index != null) && enable) {
            this.client = createClient(url);
            this.buffer1 = new Buffer(countMax, sizeMax, bulkCountMax, bulkSizeMax, bulkRetryCount, bulkRetryDelay);
            this.buffer2 = new Buffer(countMax, sizeMax, bulkCountMax, bulkSizeMax, bulkRetryCount, bulkRetryDelay);
            if (setDefaultUncaughtExceptionHandler) {
                try {
                    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            try {
                                Logger l = LogManager.getLogger(ElasticSearchAppender.class);
                                l.error("Uncaught exception: ", e);
                            } catch (Throwable ex) {
                                ElasticSearchAppender.logSystem(ElasticSearchAppender.this.out, ElasticSearchAppender.class, ex.getMessage());
                            }
                        }
                    });
                } catch (Exception e) {
                    ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, e.getMessage());
                }
            }
            this.flushThread = new Thread(String.format("log4j2-elasticsearch-appender-flush-%s", name)) {
                @Override
                public void run() {
                    final AtomicBoolean enabled = ElasticSearchAppender.this.enabled;
                    final AtomicBoolean flag = ElasticSearchAppender.this.flag;
                    final AtomicLong totalCount = ElasticSearchAppender.this.totalCount;
                    final AtomicLong totalSize = ElasticSearchAppender.this.totalSize;
                    final AtomicLong lostCount = ElasticSearchAppender.this.lostCount;
                    final AtomicLong lostSize = ElasticSearchAppender.this.lostSize;
                    final String name = ElasticSearchAppender.this.getName();
                    final String url = ElasticSearchAppender.this.url;
                    final String index = ElasticSearchAppender.this.index;
                    final boolean enable = ElasticSearchAppender.this.enable;
                    final int countMax = ElasticSearchAppender.this.countMax;
                    final long sizeMax = ElasticSearchAppender.this.sizeMax;
                    final int bulkCountMax = ElasticSearchAppender.this.bulkCountMax;
                    final long bulkSizeMax = ElasticSearchAppender.this.bulkSizeMax;
                    final long delayMax = ElasticSearchAppender.this.delayMax;
                    final int bulkRetryCount = ElasticSearchAppender.this.bulkRetryCount;
                    final long bulkRetryDelay = ElasticSearchAppender.this.bulkRetryDelay;
                    final int lengthStringMax = ElasticSearchAppender.this.lengthStringMax;
                    final boolean out = ElasticSearchAppender.this.out;
                    final boolean setDefaultUncaughtExceptionHandler = ElasticSearchAppender.this.setDefaultUncaughtExceptionHandler;
                    final RestHighLevelClient client = ElasticSearchAppender.this.client;
                    final Buffer buffer1 = ElasticSearchAppender.this.buffer1;
                    final Buffer buffer2 = ElasticSearchAppender.this.buffer2;
                    try {
                        ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, String.format("Log4j2 ElasticSearch Appender is started: name='%s' url='%s' index='%s' enable=%b countMax=%d sizeMax=%d bulkCountMax=%d bulkSizeMax=%d delayMax=%d bulkRetryCount=%d bulkRetryDelay=%d lengthStringMax=%d out=%b setDefaultUncaughtExceptionHandler=%b",
                                                                                                        name, url, index, enable,
                                                                                                        countMax, sizeMax, bulkCountMax, bulkSizeMax, delayMax, bulkRetryCount, bulkRetryDelay, lengthStringMax,
                                                                                                        out, setDefaultUncaughtExceptionHandler));
                        long pt = System.currentTimeMillis();
                        while (enabled.get()) {
                            long t = System.currentTimeMillis();
                            if (t >= pt + delayMax) {
                                if (flag.get()) {
                                    try {
                                        flag.set(false);
                                        buffer1.flush(enabled, client, url, index, lostCount, lostSize, out);
                                    } catch (Throwable e) {
                                        ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, e.getMessage());
                                    }
                                } else {
                                    try {
                                        flag.set(true);
                                        buffer2.flush(enabled, client, url, index, lostCount, lostSize, out);
                                    } catch (Throwable e) {
                                        ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, e.getMessage());
                                    }
                                }
                                pt = t;
                            }
                            if (!buffer1.isReady()) {
                                try {
                                    flag.set(false);
                                    buffer1.flush(enabled, client, url, index, lostCount, lostSize, out);
                                } catch (Throwable e) {
                                    ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, e.getMessage());
                                }
                            }
                            if (!buffer2.isReady()) {
                                try {
                                    flag.set(true);
                                    buffer2.flush(enabled, client, url, index, lostCount, lostSize, out);
                                } catch (Throwable e) {
                                    ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, e.getMessage());
                                }
                            }
                            if (enabled.get()) {
                                try {
                                    Thread.sleep(1000L);
                                } catch (InterruptedException e) {
                                }
                            }
                        }
                        try {
                            append(new InputLogEvent(false,
                                                     totalCount,
                                                     totalSize,
                                                     lostCount.get(),
                                                     lostSize.get(),
                                                     name,
                                                     url,
                                                     index,
                                                     enable,
                                                     countMax,
                                                     sizeMax,
                                                     bulkCountMax,
                                                     bulkSizeMax,
                                                     delayMax,
                                                     bulkRetryCount,
                                                     bulkRetryDelay,
                                                     lengthStringMax,
                                                     out,
                                                     setDefaultUncaughtExceptionHandler));
                        } catch (Throwable e) {
                            ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, e.getMessage());
                        }
                        try {
                            flag.set(false);
                            buffer1.flush(enabled, client, url, index, lostCount, lostSize, out);
                        } catch (Throwable e) {
                            ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, e.getMessage());
                        }
                        try {
                            flag.set(true);
                            buffer2.flush(enabled, client, url, index, lostCount, lostSize, out);
                        } catch (Throwable e) {
                            ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, e.getMessage());
                        }
                    } finally {
                        ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, String.format("Log4j2 ElasticSearch Appender is finished: name='%s' url='%s' index='%s' enable=%b countMax=%d sizeMax=%d bulkCountMax=%d bulkSizeMax=%d delayMax=%d bulkRetryCount=%d bulkRetryDelay=%d lengthStringMax=%d out=%b setDefaultUncaughtExceptionHandler=%b totalCount=%d totalSize=%d lostCount=%d lostSize=%d",
                                                                                                        name, url, index, enable,
                                                                                                        countMax, sizeMax, bulkCountMax, bulkSizeMax, delayMax, bulkRetryCount, bulkRetryDelay, lengthStringMax,
                                                                                                        out, setDefaultUncaughtExceptionHandler,
                                                                                                        totalCount.get(), totalSize.get(), lostCount.get(), lostSize.get()));
                    }
                }
            };
            try {
                Runtime.getRuntime().addShutdownHook(new Thread(String.format("log4j2-elasticsearch-appender-shutdown-%s", name)) {
                    @Override
                    public void run() {
                        final AtomicBoolean enabled = ElasticSearchAppender.this.enabled;
                        final Thread flushThread = ElasticSearchAppender.this.flushThread;
                        enabled.set(false);
                        if (flushThread.isAlive()) {
                            flushThread.interrupt();
                            try {
                                flushThread.join();
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                });
            } catch (Exception e) {
                ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, e.getMessage());
            }
            append(new InputLogEvent(true,
                                     totalCount,
                                     totalSize,
                                     lostCount.get(),
                                     lostSize.get(),
                                     name,
                                     url,
                                     index,
                                     enable,
                                     countMax,
                                     sizeMax,
                                     bulkCountMax,
                                     bulkSizeMax,
                                     delayMax,
                                     bulkRetryCount,
                                     bulkRetryDelay,
                                     lengthStringMax,
                                     out,
                                     setDefaultUncaughtExceptionHandler));
            ElasticSearchAppender.logSystem(out, ElasticSearchAppender.class, String.format("Log4j2 ElasticSearch Appender is initialized: name='%s' url='%s' index='%s' enable=%b countMax=%d sizeMax=%d bulkCountMax=%d bulkSizeMax=%d delayMax=%d bulkRetryCount=%d bulkRetryDelay=%d lengthStringMax=%d out=%b setDefaultUncaughtExceptionHandler=%b",
                                                                                            name, url, index, enable,
                                                                                            countMax, sizeMax, bulkCountMax, bulkSizeMax, delayMax, bulkRetryCount, bulkRetryDelay, lengthStringMax,
                                                                                            out, setDefaultUncaughtExceptionHandler));
        } else {
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

    @Override
    public void start() {
        super.start();
        if (flushThread != null) {
            enabled.set(true);
            flushThread.setDaemon(false);
            flushThread.start();
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (flushThread != null) {
            enabled.set(false);
            if (flushThread.isAlive()) {
                flushThread.interrupt();
                try {
                    flushThread.join();
                } catch (InterruptedException e) {
                }
            }
        }
    }

    @Override
    public void append(LogEvent event) {
        if (enabled.get()) {
            String l = event.getLoggerName();
            if (l != null) {
                if (l.startsWith("org.apache.http.")) {
                    return;
                }
            }
            append(new InputLogEvent(event, totalCount, totalSize, lostCount.get(), lostSize.get(), lengthStringMax));
        }
    }

    private void append(InputLogEvent event) {
        if (flag.get()) {
            if (!buffer1.append(event)) {
                if (!buffer2.append(event)) {
                    lostCount.incrementAndGet();
                    lostSize.addAndGet(event.size);
                }
            }
        } else {
            if (!buffer2.append(event)) {
                if (!buffer1.append(event)) {
                    lostCount.incrementAndGet();
                    lostSize.addAndGet(event.size);
                }
            }
        }
    }

    @PluginFactory
    public static ElasticSearchAppender createAppender(@PluginAttribute("name") String name,
                                                       @PluginAttribute("url") String url,
                                                       @PluginAttribute("index") String index,
                                                       @PluginAttribute("enable") String enable,
                                                       @PluginAttribute("countMax") String countMax,
                                                       @PluginAttribute("sizeMax") String sizeMax,
                                                       @PluginAttribute("bulkCountMax") String bulkCountMax,
                                                       @PluginAttribute("bulkSizeMax") String bulkSizeMax,
                                                       @PluginAttribute("delayMax") String delayMax,
                                                       @PluginAttribute("bulkRetryCount") String bulkRetryCount,
                                                       @PluginAttribute("bulkRetryDelay") String bulkRetryDelay,
                                                       @PluginAttribute("lengthStringMax") String lengthStringMax,
                                                       @PluginAttribute("out") String out,
                                                       @PluginAttribute("setDefaultUncaughtExceptionHandler") String setDefaultUncaughtExceptionHandler,
                                                       @PluginElement("Filter") Filter filter,
                                                       @PluginElement("Layout") Layout<? extends Serializable> layout) {
        return new ElasticSearchAppender((name != null) ? name : "elasticsearch",
                                         getProperty("log4j2.elasticsearch.url", "LOG4J2_ELASTICSEARCH_URL", url, null),
                                         getProperty("log4j2.elasticsearch.index", "LOG4J2_ELASTICSEARCH_INDEX", index, null),
                                         Boolean.parseBoolean(getProperty("log4j2.elasticsearch.enable", "LOG4J2_ELASTICSEARCH_ENABLE", enable, "true")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.count.max", "LOG4J2_ELASTICSEARCH_COUNT_MAX", countMax, "10000")),
                                         Long.parseLong(getProperty("log4j2.elasticsearch.size.max", "LOG4J2_ELASTICSEARCH_SIZE_MAX", sizeMax, "5242880")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.bulk.count.max", "LOG4J2_ELASTICSEARCH_BULK_COUNT_MAX", bulkCountMax, "4000")),
                                         Long.parseLong(getProperty("log4j2.elasticsearch.bulk.size.max", "LOG4J2_ELASTICSEARCH_BULK_SIZE_MAX", bulkSizeMax, "2097152")),
                                         Long.parseLong(getProperty("log4j2.elasticsearch.delay.max", "LOG4J2_ELASTICSEARCH_DELAY_MAX", delayMax, "60")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.bulk.retry.count", "LOG4J2_ELASTICSEARCH_BULK_RETRY_COUNT", bulkRetryCount, "5")),
                                         Long.parseLong(getProperty("log4j2.elasticsearch.bulk.retry.delay", "LOG4J2_ELASTICSEARCH_BULK_RETRY_DELAY", bulkRetryDelay, "5")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.length.string.max", "LOG4J2_ELASTICSEARCH_LENGTH_STRING_MAX", lengthStringMax, "65536")),
                                         Boolean.parseBoolean(getProperty("log4j2.elasticsearch.out", "LOG4J2_ELASTICSEARCH_OUT", out, "true")),
                                         Boolean.parseBoolean(getProperty("log4j2.elasticsearch.set.default.uncaught.exception.handler", "LOG4J2_ELASTICSEARCH_SET_DEFAULT_UNCAUGHT_EXCEPTION_HANDLER", setDefaultUncaughtExceptionHandler, "true")),
                                         filter,
                                         layout);
    }

    public static void logSystem(boolean out, Class clazz, String message) {
        if (out) {
            if (clazz == null) {
                clazz = ElasticSearchAppender.class;
            }
            if (message == null) {
                message = "null";
            }
            String t = String.format("%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL", System.currentTimeMillis());
            System.out.println(String.format("%s [%s] [%s] SYSTEM - %s", t, Thread.currentThread().getName(), clazz.getSimpleName(), message));
        }
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

    private static UUID createProcessUUID() {
        return new UUID(new SecureRandom().nextLong(), 0L);
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
        if (url == null) {
            throw new IllegalArgumentException("url can't be null");
        }
        String[] us = url.split(";");
        if (us.length <= 0) {
            throw new IllegalArgumentException("url should contain at least one URL to ElasticSearch host");
        }
        try {
            HttpHost[] hs = new HttpHost[us.length];
            for (int i = 0, ci = us.length; i < ci; ++i) {
                URL ur = new URL(us[i].trim());
                hs[i] = new HttpHost(ur.getHost(), ur.getPort(), ur.getProtocol());
            }
            return new RestHighLevelClient(RestClient.builder(hs));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }
}

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
    public static final long START = System.currentTimeMillis();
    public static final String PROCESS = ManagementFactory.getRuntimeMXBean().getName();
    public static final Map<String, String> LOGTAGS = createLogTags();
    public static final String HOST_NAME = createHostName();
    public static final String HOST_IP = createHostIP();
    public static final String VARIABLES = createVariables();
    public static final String PROPERTIES = createProperties();

    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean flag = new AtomicBoolean(true);
    private final AtomicLong lost = new AtomicLong(0L);
    private final AtomicLong lostSince = new AtomicLong(System.currentTimeMillis());
    private final RestHighLevelClient client;
    private final String group;
    private final int capacity;
    private final Buffer buffer1;
    private final Buffer buffer2;
    private final int length;
    private final long span;
    private final Thread flushThread;

    public ElasticSearchAppender(String name,
                                 String url,
                                 String group,
                                 int capacity,
                                 int length,
                                 long span,
                                 Filter filter,
                                 Layout<? extends Serializable> layout) {
        super(name, filter, (layout != null) ? layout : PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);

        if (group != null) {
            this.client = createClient(url);
            this.group = group;
            this.capacity = capacity;
            this.buffer1 = new Buffer(capacity);
            this.buffer2 = new Buffer(capacity);
            this.length = length;
            this.span = span * 1000L;
            this.flushThread = new Thread(String.format("log4j2-elasticsearch-appender-flush-%s", name)) {
                @Override
                public void run() {
                    Runtime.getRuntime().addShutdownHook(new Thread(String.format("log4j2-elasticsearch-appender-shutdown-%s", name)) {
                        @Override
                        public void run() {
                            enabled.set(false);
                            flushThread.interrupt();
                            try {
                                flushThread.join();
                            } catch (InterruptedException e) {
                            }
                        }
                    });
                    while (enabled.get()) {
                        try {
                            if (buffer1.isReady()) {
                                try {
                                    Thread.sleep(ElasticSearchAppender.this.span);
                                } catch (InterruptedException e) {
                                }
                            }
                            flag.set(false);
                            buffer1.flush(client, ElasticSearchAppender.this.group, lost, lostSince);
                        } catch (Throwable e) {
                        }
                        try {
                            if (buffer2.isReady()) {
                                try {
                                    Thread.sleep(ElasticSearchAppender.this.span);
                                } catch (InterruptedException e) {
                                }
                            }
                            flag.set(true);
                            buffer2.flush(client, ElasticSearchAppender.this.group, lost, lostSince);
                        } catch (Throwable e) {
                        }
                    }
                    try {
                        append(new InputLogEvent(false));
                    } catch (Throwable e) {
                    }
                    try {
                        flag.set(false);
                        buffer1.flush(client, ElasticSearchAppender.this.group, lost, lostSince);
                    } catch (Throwable e) {
                    }
                    try {
                        flag.set(true);
                        buffer2.flush(client, ElasticSearchAppender.this.group, lost, lostSince);
                    } catch (Throwable e) {
                    }
                }
            };
            append(new InputLogEvent(true));
        } else {
            this.group = null;
            this.client = null;
            this.capacity = 0;
            this.buffer1 = null;
            this.buffer2 = null;
            this.length = 0;
            this.span = 0L;
            this.flushThread = null;
        }
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public String getGroup() {
        return group;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getLength() {
        return length;
    }

    public long getSpan() {
        return span;
    }

    @Override
    public void start() {
        super.start();
        if (group != null) {
            enabled.set(true);
            flushThread.setDaemon(false);
            flushThread.start();
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (group != null) {
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
            append(new InputLogEvent(event, length));
        }
    }

    private void append(InputLogEvent event) {
        if (flag.get()) {
            if (!buffer1.append(event, flushThread)) {
                if (!buffer2.append(event, flushThread)) {
                    lost.incrementAndGet();
                }
            }
        } else {
            if (!buffer2.append(event, flushThread)) {
                if (!buffer1.append(event, flushThread)) {
                    lost.incrementAndGet();
                }
            }
        }
    }

    @PluginFactory
    public static ElasticSearchAppender createAppender(@PluginAttribute("name") String name,
                                                       @PluginAttribute("url") String url,
                                                       @PluginAttribute("group") String group,
                                                       @PluginAttribute("capacity") String capacity,
                                                       @PluginAttribute("length") String length,
                                                       @PluginAttribute("span") String span,
                                                       @PluginElement("Filter") Filter filter,
                                                       @PluginElement("Layout") Layout<? extends Serializable> layout) {
        return new ElasticSearchAppender((name != null) ? name : "elasticsearch",
                                         getProperty("log4j2.elasticsearch.url", "LOG4J2_ELASTICSEARCH_URL", url, null),
                                         getProperty("log4j2.elasticsearch.group", "LOG4J2_ELASTICSEARCH_GROUP", group, null),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.capacity", "LOG4J2_ELASTICSEARCH_CAPACITY", capacity, "10000")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.length", "LOG4J2_ELASTICSEARCH_LENGTH", length, "4096")),
                                         Long.parseLong(getProperty("log4j2.elasticsearch.span", "LOG4J2_ELASTICSEARCH_SPAN", span, "60")),
                                         filter,
                                         layout);
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
                if ((k.length() > PREFIX_EV.length()) && k.startsWith(PREFIX_EV)) {
                    lts.put(k.substring(PREFIX_EV.length()).toLowerCase(), v);
                }
            }
        }
        for (Map.Entry<Object, Object> e : sps.entrySet()) {
            Object k = e.getKey();
            Object v = e.getValue();
            if ((k != null) && (v != null)) {
                if ((k instanceof String) && (v instanceof String)) {
                    String ks = (String) k;
                    String vs = (String) v;
                    if ((ks.length() > PREFIX_SP.length()) && ks.startsWith(PREFIX_SP)) {
                        lts.put(ks.substring(PREFIX_SP.length()).toLowerCase(), vs);
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

    private static String createVariables() {
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

    private static String createProperties() {
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

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
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
@Plugin(name = "ElasticSearch", category = "Core", elementType = "appender", printObject = true)
public final class ElasticSearchAppender extends AbstractAppender {
    public static final String INSTANCE = retrieveInstance();

    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean flag = new AtomicBoolean(true);
    private final AtomicLong lost = new AtomicLong(0L);
    private final RestHighLevelClient client;
    private final String group;
    private final String stream;
    private final int capacity;
    private final Buffer buffer1;
    private final Buffer buffer2;
    private final int length;
    private final int span;
    private final FlushWait flushWait;
    private final Thread flushThread;
    private FlushInfo flushInfo;

    public ElasticSearchAppender(String name,
                                 String url,
                                 String group,
                                 String streamPrefix,
                                 String streamPostfix,
                                 int capacity,
                                 int length,
                                 int span,
                                 Filter filter,
                                 Layout<? extends Serializable> layout) {
        super(name, filter, (layout != null) ? layout : PatternLayout.createDefaultLayout(), false);

        if (group != null) {
            this.client = initClient(url);
            this.group = group;
            this.stream = initStream(streamPrefix, streamPostfix);
            if (!checkGroup(group, client)) {
                throw new RuntimeException(String.format("Group '%s' is not found", group));
            }
            this.capacity = capacity;
            this.buffer1 = new Buffer(capacity);
            this.buffer2 = new Buffer(capacity);
            this.length = length;
            this.span = span;
            this.flushWait = new FlushWait(span);
            this.flushThread = new Thread(String.format("log4j2-elasticsearch-appender-flush-%s", name)) {
                @Override
                public void run() {
                    while (enabled.get()) {
                        try {
                            flushWait.await(enabled, buffer1, buffer2);
                            flag.set(false);
                            flushInfo = buffer1.flush(client, ElasticSearchAppender.this.group, stream, flushInfo, lost);
                        } catch (Throwable e) {
                        }
                        try {
                            flushWait.await(enabled, buffer1, buffer2);
                            flag.set(true);
                            flushInfo = buffer2.flush(client, ElasticSearchAppender.this.group, stream, flushInfo, lost);
                        } catch (Throwable e) {
                        }
                    }
                    try {
                        flag.set(false);
                        flushInfo = buffer1.flush(client, ElasticSearchAppender.this.group, stream, flushInfo, lost);
                    } catch (Throwable e) {
                    }
                    try {
                        flag.set(true);
                        flushInfo = buffer2.flush(client, ElasticSearchAppender.this.group, stream, flushInfo, lost);
                    } catch (Throwable e) {
                    }
                }
            };
            this.flushInfo = new FlushInfo(0L, checkStream(group, stream, client));
        } else {
            this.group = null;
            this.stream = null;
            this.client = null;
            this.capacity = 0;
            this.buffer1 = null;
            this.buffer2 = null;
            this.length = 0;
            this.span = 0;
            this.flushWait = null;
            this.flushThread = null;
            this.flushInfo = null;
        }
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public String getGroup() {
        return group;
    }

    public String getStream() {
        return stream;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getLength() {
        return length;
    }

    public int getSpan() {
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
            flushWait.signalAll();
            try {
                flushThread.join();
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void append(LogEvent event) {
        if (enabled.get()) {
            InputLogEvent e = new InputLogEvent("", event, length);
            if (flag.get()) {
                if (!buffer1.append(e, flushWait)) {
                    if (!buffer2.append(e, flushWait)) {
                        lost.incrementAndGet();
                    }
                }
            } else {
                if (!buffer2.append(e, flushWait)) {
                    if (!buffer1.append(e, flushWait)) {
                        lost.incrementAndGet();
                    }
                }
            }
        }
    }

    @PluginFactory
    public static ElasticSearchAppender createAppender(@PluginAttribute("name") String name,
                                                       @PluginAttribute("url") String url,
                                                       @PluginAttribute("group") String group,
                                                       @PluginAttribute("streamPrefix") String streamPrefix,
                                                       @PluginAttribute("streamPostfix") String streamPostfix,
                                                       @PluginAttribute("capacity") String capacity,
                                                       @PluginAttribute("length") String length,
                                                       @PluginAttribute("span") String span,
                                                       @PluginElement("Filter") Filter filter,
                                                       @PluginElement("Layout") Layout<? extends Serializable> layout) {
        return new ElasticSearchAppender((name != null) ? name : "elasticsearch",
                                         getProperty("log4j2.elasticsearch.url", "LOG4J2_ELASTICSEARCH_URL", url, null),
                                         getProperty("log4j2.elasticsearch.group", "LOG4J2_ELASTICSEARCH_GROUP", group, null),
                                         getProperty("log4j2.elasticsearch.stream.prefix", "LOG4J2_ELASTICSEARCH_STREAM_PREFIX", streamPrefix, null),
                                         getProperty("log4j2.elasticsearch.stream.postfix", "LOG4J2_ELASTICSEARCH_STREAM_POSTFIX", streamPostfix, null),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.capacity", "LOG4J2_ELASTICSEARCH_CAPACITY", capacity, "10000")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.length", "LOG4J2_ELASTICSEARCH_LENGTH", length, "4096")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.span", "LOG4J2_ELASTICSEARCH_SPAN", span, "60")),
                                         filter,
                                         layout);
    }

    private static String retrieveInstance() {
        try {
            URL url = new URL("http://169.254.169.254/latest/meta-data/instance-id");
            URLConnection conn = url.openConnection();
            conn.setConnectTimeout(10000);
            try (InputStream in = conn.getInputStream()) {
                BufferedReader r = new BufferedReader(new InputStreamReader(in));
                String instance = r.readLine();
                if (instance != null) {
                    return instance;
                } else {
                    throw new IOException("Instance is null");
                }
            }
        } catch (IOException e) {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e1) {
                throw new RuntimeException(e1);
            }
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

    private static String initStream(String prefix, String postfix) {
        String s = INSTANCE;
        if (prefix != null) {
            s = String.format("%s/%s", prefix, s);
        }
        if (postfix != null) {
            s = String.format("%s/%s", s, postfix);
        }
        return s;
    }

    private static RestHighLevelClient initClient(String url) {
        try {
            URL u = new URL(url);
            return new RestHighLevelClient(RestClient.builder(new HttpHost(u.getHost(), u.getPort(), u.getProtocol())));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static boolean checkGroup(String group, RestHighLevelClient client) {
        return false;
    }

    private static String checkStream(String group, String stream, RestHighLevelClient client) {
        return null;
    }
}

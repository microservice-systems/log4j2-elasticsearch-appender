/*
 * Copyright (C) 2020 Microservice Systems, Inc.
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
final class ElasticSearchAppenderSingleton {
    private static final AtomicReference<ElasticSearchAppenderSingleton> INSTANCE = new AtomicReference<>(null);
    private static final Object INSTANCE_LOCK = new Object();

    public static final long PROCESS_ID = createProcessID();
    public static final UUID PROCESS_UUID = createProcessUUID();
    public static final long PROCESS_START = createProcessStart();
    public static final String HOST_NAME = createHostName();
    public static final String HOST_IP = createHostIP();
    public static final Map<String, String> LOG_TAGS = createLogTags();

    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean flag = new AtomicBoolean(true);
    private final AtomicLong totalCount = new AtomicLong(0L);
    private final AtomicLong totalSize = new AtomicLong(0L);
    private final AtomicLong lostCount = new AtomicLong(0L);
    private final AtomicLong lostSize = new AtomicLong(0L);
    private final String name;
    private final String url;
    private final String user;
    private final String index;
    private final boolean enable;
    private final int countMax;
    private final long sizeMax;
    private final int bulkCountMax;
    private final long bulkSizeMax;
    private final long delayMax;
    private final int bulkRetryCount;
    private final long bulkRetryDelay;
    private final int eventSizeStartFinish;
    private final int eventSizeDefault;
    private final int eventSizeException;
    private final int lengthStringMax;
    private final boolean out;
    private final boolean debug;
    private final boolean setDefaultUncaughtExceptionHandler;
    private final RestHighLevelClient client;
    private final Buffer buffer1;
    private final Buffer buffer2;
    private final Thread flushThread;

    public ElasticSearchAppenderSingleton(String name,
                                          String url,
                                          String user,
                                          String password,
                                          String index,
                                          boolean enable,
                                          int countMax,
                                          long sizeMax,
                                          int bulkCountMax,
                                          long bulkSizeMax,
                                          long delayMax,
                                          int bulkRetryCount,
                                          long bulkRetryDelay,
                                          int eventSizeStartFinish,
                                          int eventSizeDefault,
                                          int eventSizeException,
                                          int lengthStringMax,
                                          boolean out,
                                          boolean debug,
                                          boolean setDefaultUncaughtExceptionHandler) {
        if (countMax < 1000) {
            throw new IllegalArgumentException("Argument countMax can't be less than 1000");
        }
        if (sizeMax < 524288L) {
            throw new IllegalArgumentException("Argument sizeMax can't be less than 524288");
        }
        if (bulkCountMax < 400) {
            throw new IllegalArgumentException("Argument bulkCountMax can't be less than 400");
        }
        if (bulkSizeMax < 209715L) {
            throw new IllegalArgumentException("Argument bulkSizeMax can't be less than 209715");
        }
        if (delayMax < 10000L) {
            throw new IllegalArgumentException("Argument delayMax can't be less than 10 seconds");
        }
        if (bulkRetryCount < 1) {
            throw new IllegalArgumentException("Argument bulkRetryCount can't be less than 1");
        }
        if (bulkRetryDelay < 1000L) {
            throw new IllegalArgumentException("Argument bulkRetryDelay can't be less than 1 second");
        }
        if (eventSizeStartFinish < 256) {
            throw new IllegalArgumentException("Argument eventSizeStartFinish can't be less than 256");
        }
        if (eventSizeDefault < 256) {
            throw new IllegalArgumentException("Argument eventSizeDefault can't be less than 256");
        }
        if (eventSizeException < 256) {
            throw new IllegalArgumentException("Argument eventSizeException can't be less than 256");
        }
        if (lengthStringMax < 128) {
            throw new IllegalArgumentException("Argument lengthStringMax can't be less than 128");
        }

        this.name = name;
        this.url = url;
        this.user = user;
        this.index = index;
        this.enable = enable;
        this.countMax = countMax;
        this.sizeMax = sizeMax;
        this.bulkCountMax = bulkCountMax;
        this.bulkSizeMax = bulkSizeMax;
        this.delayMax = delayMax;
        this.bulkRetryCount = bulkRetryCount;
        this.bulkRetryDelay = bulkRetryDelay;
        this.eventSizeStartFinish = eventSizeStartFinish;
        this.eventSizeDefault = eventSizeDefault;
        this.eventSizeException = eventSizeException;
        this.lengthStringMax = lengthStringMax;
        this.out = out;
        this.debug = debug;
        this.setDefaultUncaughtExceptionHandler = setDefaultUncaughtExceptionHandler;

        if ((url != null) && (index != null) && enable) {
            this.client = createClient(url, user, password);
            this.buffer1 = new Buffer(countMax, sizeMax, bulkCountMax, bulkSizeMax, bulkRetryCount, bulkRetryDelay);
            this.buffer2 = new Buffer(countMax, sizeMax, bulkCountMax, bulkSizeMax, bulkRetryCount, bulkRetryDelay);
            if (setDefaultUncaughtExceptionHandler) {
                try {
                    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            try {
                                Logger l = LogManager.getLogger(ElasticSearchAppenderSingleton.class);
                                l.error("Uncaught exception: ", e);
                            } catch (Throwable ex) {
                                ElasticSearchAppenderSingleton.logSystem(ElasticSearchAppenderSingleton.this.out, ElasticSearchAppenderSingleton.class, ex.getMessage());
                            }
                        }
                    });
                } catch (Exception e) {
                    ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, e.getMessage());
                }
            }
            this.flushThread = new Thread(String.format("log4j2-elasticsearch-appender-flush-%s", name)) {
                @Override
                public void run() {
                    final AtomicBoolean enabled = ElasticSearchAppenderSingleton.this.enabled;
                    final AtomicBoolean flag = ElasticSearchAppenderSingleton.this.flag;
                    final AtomicLong totalCount = ElasticSearchAppenderSingleton.this.totalCount;
                    final AtomicLong totalSize = ElasticSearchAppenderSingleton.this.totalSize;
                    final AtomicLong lostCount = ElasticSearchAppenderSingleton.this.lostCount;
                    final AtomicLong lostSize = ElasticSearchAppenderSingleton.this.lostSize;
                    final String name = ElasticSearchAppenderSingleton.this.getName();
                    final String url = ElasticSearchAppenderSingleton.this.url;
                    final String user = ElasticSearchAppenderSingleton.this.user;
                    final String index = ElasticSearchAppenderSingleton.this.index;
                    final boolean enable = ElasticSearchAppenderSingleton.this.enable;
                    final int countMax = ElasticSearchAppenderSingleton.this.countMax;
                    final long sizeMax = ElasticSearchAppenderSingleton.this.sizeMax;
                    final int bulkCountMax = ElasticSearchAppenderSingleton.this.bulkCountMax;
                    final long bulkSizeMax = ElasticSearchAppenderSingleton.this.bulkSizeMax;
                    final long delayMax = ElasticSearchAppenderSingleton.this.delayMax;
                    final int bulkRetryCount = ElasticSearchAppenderSingleton.this.bulkRetryCount;
                    final long bulkRetryDelay = ElasticSearchAppenderSingleton.this.bulkRetryDelay;
                    final int eventSizeStartFinish = ElasticSearchAppenderSingleton.this.eventSizeStartFinish;
                    final int eventSizeDefault = ElasticSearchAppenderSingleton.this.eventSizeDefault;
                    final int eventSizeException = ElasticSearchAppenderSingleton.this.eventSizeException;
                    final int lengthStringMax = ElasticSearchAppenderSingleton.this.lengthStringMax;
                    final boolean out = ElasticSearchAppenderSingleton.this.out;
                    final boolean debug = ElasticSearchAppenderSingleton.this.debug;
                    final boolean setDefaultUncaughtExceptionHandler = ElasticSearchAppenderSingleton.this.setDefaultUncaughtExceptionHandler;
                    final RestHighLevelClient client = ElasticSearchAppenderSingleton.this.client;
                    final Buffer buffer1 = ElasticSearchAppenderSingleton.this.buffer1;
                    final Buffer buffer2 = ElasticSearchAppenderSingleton.this.buffer2;
                    try {
                        ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, String.format("Log4j2 ElasticSearch Appender is started: name='%s' url='%s' user='%s' index='%s' enable=%b countMax=%d sizeMax=%d bulkCountMax=%d bulkSizeMax=%d delayMax=%d bulkRetryCount=%d bulkRetryDelay=%d eventSizeStartFinish=%d eventSizeDefault=%d eventSizeException=%d lengthStringMax=%d out=%b debug=%b setDefaultUncaughtExceptionHandler=%b",
                                                                                                        name, url, user, index, enable,
                                                                                                        countMax, sizeMax, bulkCountMax, bulkSizeMax, delayMax, bulkRetryCount, bulkRetryDelay, eventSizeStartFinish, eventSizeDefault, eventSizeException, lengthStringMax,
                                                                                                        out, debug, setDefaultUncaughtExceptionHandler));
                        long pt = System.currentTimeMillis();
                        while (enabled.get()) {
                            long t = System.currentTimeMillis();
                            if (t >= pt + delayMax) {
                                if (flag.get()) {
                                    try {
                                        flag.set(false);
                                        buffer1.flush(enabled, client, name, url, user, index, 1, lostCount, lostSize, out, debug);
                                    } catch (Throwable e) {
                                        ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, e.getMessage());
                                    }
                                } else {
                                    try {
                                        flag.set(true);
                                        buffer2.flush(enabled, client, name, url, user, index, 2, lostCount, lostSize, out, debug);
                                    } catch (Throwable e) {
                                        ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, e.getMessage());
                                    }
                                }
                                pt = t;
                            }
                            if (!buffer1.isReady()) {
                                try {
                                    flag.set(false);
                                    buffer1.flush(enabled, client, name, url, user, index, 1, lostCount, lostSize, out, debug);
                                } catch (Throwable e) {
                                    ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, e.getMessage());
                                }
                            }
                            if (!buffer2.isReady()) {
                                try {
                                    flag.set(true);
                                    buffer2.flush(enabled, client, name, url, user, index, 2, lostCount, lostSize, out, debug);
                                } catch (Throwable e) {
                                    ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, e.getMessage());
                                }
                            }
                            if (!Util.delay(enabled, 1000L, 600L)) {
                                break;
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
                                                     user,
                                                     index,
                                                     enable,
                                                     countMax,
                                                     sizeMax,
                                                     bulkCountMax,
                                                     bulkSizeMax,
                                                     delayMax,
                                                     bulkRetryCount,
                                                     bulkRetryDelay,
                                                     eventSizeStartFinish,
                                                     eventSizeDefault,
                                                     eventSizeException,
                                                     lengthStringMax,
                                                     out,
                                                     setDefaultUncaughtExceptionHandler));
                        } catch (Throwable e) {
                            ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, e.getMessage());
                        }
                        try {
                            flag.set(false);
                            buffer1.flush(enabled, client, name, url, user, index, 1, lostCount, lostSize, out, debug);
                        } catch (Throwable e) {
                            ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, e.getMessage());
                        }
                        try {
                            flag.set(true);
                            buffer2.flush(enabled, client, name, url, user, index, 2, lostCount, lostSize, out, debug);
                        } catch (Throwable e) {
                            ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, e.getMessage());
                        }
                    } finally {
                        ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, String.format("Log4j2 ElasticSearch Appender is finished: name='%s' url='%s' user='%s' index='%s' enable=%b countMax=%d sizeMax=%d bulkCountMax=%d bulkSizeMax=%d delayMax=%d bulkRetryCount=%d bulkRetryDelay=%d eventSizeStartFinish=%d eventSizeDefault=%d eventSizeException=%d lengthStringMax=%d out=%b debug=%b setDefaultUncaughtExceptionHandler=%b totalCount=%d totalSize=%d lostCount=%d lostSize=%d",
                                                                                                                          name, url, user, index, enable,
                                                                                                                          countMax, sizeMax, bulkCountMax, bulkSizeMax, delayMax, bulkRetryCount, bulkRetryDelay, eventSizeStartFinish, eventSizeDefault, eventSizeException, lengthStringMax,
                                                                                                                          out, debug, setDefaultUncaughtExceptionHandler,
                                                                                                                          totalCount.get(), totalSize.get(), lostCount.get(), lostSize.get()));
                    }
                }
            };
            try {
                Runtime.getRuntime().addShutdownHook(new Thread(String.format("log4j2-elasticsearch-appender-shutdown-%s", name)) {
                    @Override
                    public void run() {
                        final AtomicBoolean enabled = ElasticSearchAppenderSingleton.this.enabled;
                        final Thread flushThread = ElasticSearchAppenderSingleton.this.flushThread;
                        enabled.set(false);
                        if (flushThread.isAlive()) {
                            try {
                                flushThread.join();
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                });
            } catch (Exception e) {
                ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, e.getMessage());
            }
            append(new InputLogEvent(true,
                                     totalCount,
                                     totalSize,
                                     lostCount.get(),
                                     lostSize.get(),
                                     name,
                                     url,
                                     user,
                                     index,
                                     enable,
                                     countMax,
                                     sizeMax,
                                     bulkCountMax,
                                     bulkSizeMax,
                                     delayMax,
                                     bulkRetryCount,
                                     bulkRetryDelay,
                                     eventSizeStartFinish,
                                     eventSizeDefault,
                                     eventSizeException,
                                     lengthStringMax,
                                     out,
                                     setDefaultUncaughtExceptionHandler));
            ElasticSearchAppenderSingleton.logSystem(out, ElasticSearchAppenderSingleton.class, String.format("Log4j2 ElasticSearch Appender is initialized: name='%s' url='%s' user='%s' index='%s' enable=%b countMax=%d sizeMax=%d bulkCountMax=%d bulkSizeMax=%d delayMax=%d bulkRetryCount=%d bulkRetryDelay=%d eventSizeStartFinish=%d eventSizeDefault=%d eventSizeException=%d lengthStringMax=%d out=%b debug=%b setDefaultUncaughtExceptionHandler=%b",
                                                                                                              name, url, user, index, enable,
                                                                                                              countMax, sizeMax, bulkCountMax, bulkSizeMax, delayMax, bulkRetryCount, bulkRetryDelay, eventSizeStartFinish, eventSizeDefault, eventSizeException, lengthStringMax,
                                                                                                              out, debug, setDefaultUncaughtExceptionHandler));
            enabled.set(true);
            flushThread.setDaemon(false);
            flushThread.start();
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

    public long getTotalCount() {
        return totalCount.get();
    }

    public long getTotalSize() {
        return totalSize.get();
    }

    public long getLostCount() {
        return lostCount.get();
    }

    public long getLostSize() {
        return lostSize.get();
    }

    public String getName() {
        return name;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getIndex() {
        return index;
    }

    public boolean isEnable() {
        return enable;
    }

    public int getCountMax() {
        return countMax;
    }

    public long getSizeMax() {
        return sizeMax;
    }

    public int getBulkCountMax() {
        return bulkCountMax;
    }

    public long getBulkSizeMax() {
        return bulkSizeMax;
    }

    public long getDelayMax() {
        return delayMax;
    }

    public int getBulkRetryCount() {
        return bulkRetryCount;
    }

    public long getBulkRetryDelay() {
        return bulkRetryDelay;
    }

    public int getEventSizeStartFinish() {
        return eventSizeStartFinish;
    }

    public int getEventSizeDefault() {
        return eventSizeDefault;
    }

    public int getEventSizeException() {
        return eventSizeException;
    }

    public int getLengthStringMax() {
        return lengthStringMax;
    }

    public boolean isOut() {
        return out;
    }

    public boolean isDebug() {
        return debug;
    }

    public boolean isSetDefaultUncaughtExceptionHandler() {
        return setDefaultUncaughtExceptionHandler;
    }

    public void append(LogEvent event) {
        if (enabled.get()) {
            String t = event.getThreadName();
            if (t != null) {
                if (t.startsWith("log4j2-elasticsearch-appender-flush-")) {
                    return;
                }
            }
            append(new InputLogEvent(event, totalCount, totalSize, lostCount.get(), lostSize.get(), eventSizeDefault, eventSizeException, lengthStringMax));
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

    public static ElasticSearchAppenderSingleton getInstance(String name,
                                                             String url,
                                                             String user,
                                                             String password,
                                                             String index,
                                                             boolean enable,
                                                             int countMax,
                                                             long sizeMax,
                                                             int bulkCountMax,
                                                             long bulkSizeMax,
                                                             long delayMax,
                                                             int bulkRetryCount,
                                                             long bulkRetryDelay,
                                                             int eventSizeStartFinish,
                                                             int eventSizeDefault,
                                                             int eventSizeException,
                                                             int lengthStringMax,
                                                             boolean out,
                                                             boolean debug,
                                                             boolean setDefaultUncaughtExceptionHandler) {
        ElasticSearchAppenderSingleton esas = INSTANCE.get();
        if (esas != null) {
            return esas;
        } else {
            synchronized (INSTANCE_LOCK) {
                esas = INSTANCE.get();
                if (esas != null) {
                    return esas;
                } else {
                    esas = new ElasticSearchAppenderSingleton(name, url, user, password, index, enable,
                                                              countMax, sizeMax, bulkCountMax, bulkSizeMax, delayMax, bulkRetryCount, bulkRetryDelay,
                                                              eventSizeStartFinish, eventSizeDefault, eventSizeException, lengthStringMax,
                                                              out, debug, setDefaultUncaughtExceptionHandler);
                    if (INSTANCE.compareAndSet(null, esas)) {
                        return esas;
                    } else {
                        return INSTANCE.get();
                    }
                }
            }
        }
    }

    public static void logSystem(boolean out, Class clazz, String message) {
        if (out) {
            if (clazz == null) {
                clazz = ElasticSearchAppenderSingleton.class;
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

    private static long createProcessStart() {
        return ManagementFactory.getRuntimeMXBean().getStartTime();
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

    private static RestHighLevelClient createClient(String url, String user, String password) {
        if (url == null) {
            throw new IllegalArgumentException("url can't be null");
        }
        String[] us = url.split(";");
        if (us.length <= 0) {
            throw new IllegalArgumentException("url should contain at least one URL to ElasticSearch host");
        }
        try {
            URL[] hs = new URL[us.length];
            for (int i = 0, ci = us.length; i < ci; ++i) {
                hs[i] = new URL(us[i].trim());
            }
            return new RestHighLevelClient(hs, user, password);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }
}

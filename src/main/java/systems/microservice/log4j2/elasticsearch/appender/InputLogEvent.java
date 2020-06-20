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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.util.StringBuilderWriter;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.ReadOnlyStringMap;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.*;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
final class InputLogEvent extends UpdateRequest implements Comparable<InputLogEvent> {
    private static final long MB = 1048576L;
    private static final int SIZE_OVERHEAD = 64;
    private static final String PROCESS_UUID = ElasticSearchAppender.PROCESS_UUID.toString();
    private static final long MOST_SIG_BITS = ElasticSearchAppender.PROCESS_UUID.getMostSignificantBits();
    private static final AtomicLong THREAD_LEAST_SIG_BITS = new AtomicLong(0L);
    private static final AtomicLong EVENT_LEAST_SIG_BITS = new AtomicLong(0L);
    private static final ThreadLocal<String> THREAD_UUID = ThreadLocal.withInitial(() -> new UUID(MOST_SIG_BITS, THREAD_LEAST_SIG_BITS.getAndIncrement()).toString());
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();
    private static final ClassLoadingMXBean CLASS_LOADING_MX_BEAN = ManagementFactory.getClassLoadingMXBean();
    private static final CompilationMXBean COMPILATION_MX_BEAN = ManagementFactory.getCompilationMXBean();
    private static final AtomicInteger THREAD_COUNT_LIVE;
    private static final AtomicInteger THREAD_COUNT_DAEMON;
    private static final AtomicInteger THREAD_COUNT_PEAK;
    private static final AtomicInteger MEMORY_HEAP_INIT;
    private static final AtomicInteger MEMORY_HEAP_USED;
    private static final AtomicInteger MEMORY_HEAP_COMMITTED;
    private static final AtomicInteger MEMORY_HEAP_MAX;
    private static final AtomicInteger MEMORY_NON_HEAP_INIT;
    private static final AtomicInteger MEMORY_NON_HEAP_USED;
    private static final AtomicInteger MEMORY_NON_HEAP_COMMITTED;
    private static final AtomicInteger MEMORY_NON_HEAP_MAX;
    private static final AtomicInteger MEMORY_OBJECT_PENDING_FINALIZATION_COUNT;
    private static final AtomicInteger CLASS_COUNT_ACTIVE;
    private static final Thread MONITOR_THREAD;

    public final long time;
    public final int size;

    public InputLogEvent(boolean start,
                         AtomicLong totalCount,
                         AtomicLong totalSize,
                         long lostCount,
                         long lostSize,
                         String name,
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
                         boolean setDefaultUncaughtExceptionHandler) {
        super(null, new UUID(MOST_SIG_BITS, EVENT_LEAST_SIG_BITS.getAndIncrement()).toString());

        this.time = start ? ElasticSearchAppender.PROCESS_START_TIME : System.currentTimeMillis();
        this.docAsUpsert(true);

        try {
            Thread t = Thread.currentThread();
            ByteArrayOutputStream buf = new ByteArrayOutputStream(65536);
            XContentBuilder cb = XContentFactory.smileBuilder(buf);
            cb.humanReadable(true);
            cb.startObject();
            cb.timeField("time", time);
            if (start) {
                cb.field("type", "START");
            } else {
                cb.field("type", "FINISH");
            }
            cb.field("process.id", ElasticSearchAppender.PROCESS_ID);
            cb.field("process.uuid", InputLogEvent.PROCESS_UUID);
            cb.timeField("process.start.time", ElasticSearchAppender.PROCESS_START_TIME);
            if (!start) {
                cb.timeField("process.finish.time", time);
            }
            addField(cb, "process.variables", createProcessVariables(), lengthStringMax);
            addField(cb, "process.properties", createProcessProperties(), lengthStringMax);
            addField(cb, "process.cmdline", Util.loadString(String.format("/proc/%d/cmdline", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
            addField(cb, "process.io", Util.loadString(String.format("/proc/%d/io", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
            addField(cb, "process.limits", Util.loadString(String.format("/proc/%d/limits", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
            addField(cb, "process.mounts", Util.loadString(String.format("/proc/%d/mounts", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
            addField(cb, "process.net.dev", Util.loadString(String.format("/proc/%d/net/dev", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
            addField(cb, "process.net.protocols", Util.loadString(String.format("/proc/%d/net/protocols", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
            addField(cb, "host.name", ElasticSearchAppender.HOST_NAME, lengthStringMax);
            addField(cb, "host.ip", ElasticSearchAppender.HOST_IP, lengthStringMax);
            for (Map.Entry<String, String> e : ElasticSearchAppender.LOG_TAGS.entrySet()) {
                addField(cb, e.getKey(), e.getValue(), lengthStringMax);
            }
            cb.field("logger", ElasticSearchAppender.class.getName());
            cb.field("thread.id", t.getId());
            cb.field("thread.uuid", InputLogEvent.THREAD_UUID.get());
            addField(cb, "thread.name", t.getName(), lengthStringMax);
            cb.field("thread.priority", t.getPriority());
            cb.field("thread.count.live", THREAD_COUNT_LIVE.get());
            cb.field("thread.count.daemon", THREAD_COUNT_DAEMON.get());
            cb.field("thread.count.peak", THREAD_COUNT_PEAK.get());
            cb.field("thread.count.total", THREAD_MX_BEAN.getTotalStartedThreadCount());
            cb.field("memory.heap.init", MEMORY_HEAP_INIT.get());
            cb.field("memory.heap.used", MEMORY_HEAP_USED.get());
            cb.field("memory.heap.committed", MEMORY_HEAP_COMMITTED.get());
            cb.field("memory.heap.max", MEMORY_HEAP_MAX.get());
            cb.field("memory.non.heap.init", MEMORY_NON_HEAP_INIT.get());
            cb.field("memory.non.heap.used", MEMORY_NON_HEAP_USED.get());
            cb.field("memory.non.heap.committed", MEMORY_NON_HEAP_COMMITTED.get());
            cb.field("memory.non.heap.max", MEMORY_NON_HEAP_MAX.get());
            cb.field("memory.object.pending.finalization.count", MEMORY_OBJECT_PENDING_FINALIZATION_COUNT.get());
            cb.field("class.count.active", CLASS_COUNT_ACTIVE.get());
            cb.field("class.count.loaded", CLASS_LOADING_MX_BEAN.getTotalLoadedClassCount());
            cb.field("class.count.unloaded", CLASS_LOADING_MX_BEAN.getUnloadedClassCount());
            cb.field("compilation.time.total", COMPILATION_MX_BEAN.getTotalCompilationTime());
            cb.field("level", "INFO");
            if (start) {
                cb.field("message", "Hello World!");
            } else {
                cb.field("message", "Goodbye World!");
            }
            addField(cb, "appender.name", name, lengthStringMax);
            addField(cb, "appender.url", url, lengthStringMax);
            addField(cb, "appender.index", index, lengthStringMax);
            cb.field("appender.enable", enable);
            cb.field("appender.count.max", countMax);
            cb.field("appender.size.max", sizeMax);
            cb.field("appender.bulk.count.max", bulkCountMax);
            cb.field("appender.bulk.size.max", bulkSizeMax);
            cb.field("appender.delay.max", delayMax);
            cb.field("appender.bulk.retry.count", bulkRetryCount);
            cb.field("appender.bulk.retry.delay", bulkRetryDelay);
            cb.field("appender.length.string.max", lengthStringMax);
            cb.field("appender.out", out);
            cb.field("appender.set.default.uncaught.exception.handler", setDefaultUncaughtExceptionHandler);
            cb.flush();
            this.size = buf.size() + SIZE_OVERHEAD;
            cb.field("size", size);
            cb.field("total.count", totalCount.incrementAndGet());
            cb.field("total.size", totalSize.addAndGet(size));
            cb.field("lost.count", lostCount);
            cb.field("lost.size", lostSize);
            cb.endObject();
            this.doc(cb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InputLogEvent(LogEvent event,
                         AtomicLong totalCount,
                         AtomicLong totalSize,
                         long lostCount,
                         long lostSize,
                         int lengthStringMax) {
        super(null, new UUID(MOST_SIG_BITS, EVENT_LEAST_SIG_BITS.getAndIncrement()).toString());

        this.time = event.getTimeMillis();
        this.docAsUpsert(true);

        try {
            Throwable ex = event.getThrown();
            ByteArrayOutputStream buf = new ByteArrayOutputStream((ex == null) ? 1024 : 8192);
            XContentBuilder cb = XContentFactory.smileBuilder(buf);
            cb.humanReadable(true);
            cb.startObject();
            cb.timeField("time", time);
            cb.field("type", (ex == null) ? "DEFAULT" : "EXCEPTION");
            cb.field("process.id", ElasticSearchAppender.PROCESS_ID);
            cb.field("process.uuid", InputLogEvent.PROCESS_UUID);
            cb.timeField("process.start.time", ElasticSearchAppender.PROCESS_START_TIME);
            addField(cb, "host.name", ElasticSearchAppender.HOST_NAME, lengthStringMax);
            addField(cb, "host.ip", ElasticSearchAppender.HOST_IP, lengthStringMax);
            for (Map.Entry<String, String> e : ElasticSearchAppender.LOG_TAGS.entrySet()) {
                addField(cb, e.getKey(), e.getValue(), lengthStringMax);
            }
            addField(cb, "logger", event.getLoggerName(), lengthStringMax);
            cb.field("thread.id", event.getThreadId());
            cb.field("thread.uuid", InputLogEvent.THREAD_UUID.get());
            addField(cb, "thread.name", event.getThreadName(), lengthStringMax);
            cb.field("thread.priority", event.getThreadPriority());
            cb.field("thread.count.live", THREAD_COUNT_LIVE.get());
            cb.field("thread.count.daemon", THREAD_COUNT_DAEMON.get());
            cb.field("thread.count.peak", THREAD_COUNT_PEAK.get());
            cb.field("memory.heap.init", MEMORY_HEAP_INIT.get());
            cb.field("memory.heap.used", MEMORY_HEAP_USED.get());
            cb.field("memory.heap.committed", MEMORY_HEAP_COMMITTED.get());
            cb.field("memory.heap.max", MEMORY_HEAP_MAX.get());
            cb.field("memory.non.heap.init", MEMORY_NON_HEAP_INIT.get());
            cb.field("memory.non.heap.used", MEMORY_NON_HEAP_USED.get());
            cb.field("memory.non.heap.committed", MEMORY_NON_HEAP_COMMITTED.get());
            cb.field("memory.non.heap.max", MEMORY_NON_HEAP_MAX.get());
            cb.field("memory.object.pending.finalization.count", MEMORY_OBJECT_PENDING_FINALIZATION_COUNT.get());
            cb.field("class.count.active", CLASS_COUNT_ACTIVE.get());
            Level l = event.getLevel();
            if (l != null) {
                addField(cb, "level", l.toString(), lengthStringMax);
            } else {
                cb.field("level", "INFO");
            }
            Message m = event.getMessage();
            if (m != null) {
                addField(cb, "message", m.getFormattedMessage(), lengthStringMax);
            }
            StackTraceElement ste = event.getSource();
            if (ste != null) {
                addField(cb, "source.file", ste.getFileName(), lengthStringMax);
                addField(cb, "source.class", ste.getClassName(), lengthStringMax);
                addField(cb, "source.method", ste.getMethodName(), lengthStringMax);
                cb.field("source.line", ste.getLineNumber());
            }
            if (ex != null) {
                addField(cb, "exception.class", ex.getClass().getName(), lengthStringMax);
                addField(cb, "exception.message", ex.getMessage(), lengthStringMax);
                try (StringBuilderWriter sbw = new StringBuilderWriter(4096)) {
                    ex.printStackTrace(new PrintWriter(sbw, false));
                    addField(cb, "exception.stacktrace", sbw.toString(), lengthStringMax);
                }
                Throwable[] sex = ex.getSuppressed();
                if (sex != null) {
                    cb.field("exception.suppressed.count", sex.length);
                }
                Throwable cex = ex.getCause();
                if (cex != null) {
                    addField(cb, "exception.cause.class", cex.getClass().getName(), lengthStringMax);
                    addField(cb, "exception.cause.message", cex.getMessage(), lengthStringMax);
                }
            }
            Marker mrk = event.getMarker();
            if (mrk != null) {
                addField(cb, "marker.name", mrk.getName(), lengthStringMax);
                cb.field("marker.parents", mrk.hasParents());
            }
            ReadOnlyStringMap ctx = event.getContextData();
            if (ctx != null) {
                ctx.forEach((k, v) -> {
                    if ((k != null) && (v != null)) {
                        addField(cb, "context." + k, v.toString(), lengthStringMax);
                    }
                });
            }
            cb.flush();
            this.size = buf.size() + SIZE_OVERHEAD;
            cb.field("size", size);
            cb.field("total.count", totalCount.incrementAndGet());
            cb.field("total.size", totalSize.addAndGet(size));
            cb.field("lost.count", lostCount);
            cb.field("lost.size", lostSize);
            cb.endObject();
            this.doc(cb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareTo(InputLogEvent event) {
        if (time < event.time) {
            return -1;
        } else if (time > event.time) {
            return 1;
        } else {
            return 0;
        }
    }

    static {
        MemoryUsage hmu0 = MEMORY_MX_BEAN.getHeapMemoryUsage();
        MemoryUsage nhmu0 = MEMORY_MX_BEAN.getNonHeapMemoryUsage();
        THREAD_COUNT_LIVE = new AtomicInteger(THREAD_MX_BEAN.getThreadCount());
        THREAD_COUNT_DAEMON = new AtomicInteger(THREAD_MX_BEAN.getDaemonThreadCount());
        THREAD_COUNT_PEAK = new AtomicInteger(THREAD_MX_BEAN.getPeakThreadCount());
        MEMORY_HEAP_INIT = new AtomicInteger((int) (hmu0.getInit() / MB));
        MEMORY_HEAP_USED = new AtomicInteger((int) (hmu0.getUsed() / MB));
        MEMORY_HEAP_COMMITTED = new AtomicInteger((int) (hmu0.getCommitted() / MB));
        MEMORY_HEAP_MAX = new AtomicInteger((int) (hmu0.getMax() / MB));
        MEMORY_NON_HEAP_INIT = new AtomicInteger((int) (nhmu0.getInit() / MB));
        MEMORY_NON_HEAP_USED = new AtomicInteger((int) (nhmu0.getUsed() / MB));
        MEMORY_NON_HEAP_COMMITTED = new AtomicInteger((int) (nhmu0.getCommitted() / MB));
        MEMORY_NON_HEAP_MAX = new AtomicInteger((int) (nhmu0.getMax() / MB));
        MEMORY_OBJECT_PENDING_FINALIZATION_COUNT = new AtomicInteger(MEMORY_MX_BEAN.getObjectPendingFinalizationCount());
        CLASS_COUNT_ACTIVE = new AtomicInteger(CLASS_LOADING_MX_BEAN.getLoadedClassCount());
        MONITOR_THREAD = new Thread("log4j2-elasticsearch-appender-monitor") {
            @Override
            public void run() {
                while (true) {
                    try {
                        MemoryUsage hmu = MEMORY_MX_BEAN.getHeapMemoryUsage();
                        MemoryUsage nhmu = MEMORY_MX_BEAN.getNonHeapMemoryUsage();
                        THREAD_COUNT_LIVE.set(THREAD_MX_BEAN.getThreadCount());
                        THREAD_COUNT_DAEMON.set(THREAD_MX_BEAN.getDaemonThreadCount());
                        THREAD_COUNT_PEAK.set(THREAD_MX_BEAN.getPeakThreadCount());
                        MEMORY_HEAP_INIT.set((int) (hmu.getInit() / MB));
                        MEMORY_HEAP_USED.set((int) (hmu.getUsed() / MB));
                        MEMORY_HEAP_COMMITTED.set((int) (hmu.getCommitted() / MB));
                        MEMORY_HEAP_MAX.set((int) (hmu.getMax() / MB));
                        MEMORY_NON_HEAP_INIT.set((int) (nhmu.getInit() / MB));
                        MEMORY_NON_HEAP_USED.set((int) (nhmu.getUsed() / MB));
                        MEMORY_NON_HEAP_COMMITTED.set((int) (nhmu.getCommitted() / MB));
                        MEMORY_NON_HEAP_MAX.set((int) (nhmu.getMax() / MB));
                        MEMORY_OBJECT_PENDING_FINALIZATION_COUNT.set(MEMORY_MX_BEAN.getObjectPendingFinalizationCount());
                        CLASS_COUNT_ACTIVE.set(CLASS_LOADING_MX_BEAN.getLoadedClassCount());
                        try {
                            Thread.sleep(1000L);
                        } catch (InterruptedException e) {
                        }
                    } catch (Throwable e) {
                    }
                }
            }
        };
        MONITOR_THREAD.setDaemon(true);
        MONITOR_THREAD.start();
    }

    private static void addField(XContentBuilder builder, String name, String value, int lengthMax) {
        if ((name != null) && (value != null)) {
            try {
                builder.field(name, Util.cut(value, lengthMax));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static String createProcessVariables() {
        Map<String, String> evs = System.getenv();
        StringBuilder sb = new StringBuilder(32768);
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

    private static String createProcessProperties() {
        Properties sps = System.getProperties();
        StringBuilder sb = new StringBuilder(32768);
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
}

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

import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.util.StringBuilderWriter;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.ReadOnlyStringMap;

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
final class InputLogEvent implements Comparable<InputLogEvent> {
    private static final long MB = 1048576L;
    private static final int SIZE_OVERHEAD = 64;
    private static final SmileFactory SMILE_FACTORY = new SmileFactory();
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

    public final String id;
    public final long time;
    public final ByteArrayOutputStream data;
    public final int size;
    public String index = null;

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
                         int eventSizeStartFinish,
                         int eventSizeDefault,
                         int eventSizeException,
                         int lengthStringMax,
                         boolean out,
                         boolean setDefaultUncaughtExceptionHandler) {
        this.id = new UUID(MOST_SIG_BITS, EVENT_LEAST_SIG_BITS.getAndIncrement()).toString();
        this.time = start ? ElasticSearchAppender.PROCESS_START_TIME : System.currentTimeMillis();

        try {
            Thread t = Thread.currentThread();
            this.data = new ByteArrayOutputStream(eventSizeStartFinish);
            try (SmileGenerator gen = SMILE_FACTORY.createGenerator(this.data)) {
                gen.writeStartObject();
                gen.writeNumberField("time", time);
                if (start) {
                    gen.writeStringField("type", "START");
                } else {
                    gen.writeStringField("type", "FINISH");
                }
                gen.writeStringField("language", "JAVA");
                gen.writeNumberField("process.id", ElasticSearchAppender.PROCESS_ID);
                gen.writeStringField("process.uuid", InputLogEvent.PROCESS_UUID);
                gen.writeNumberField("process.start.time", ElasticSearchAppender.PROCESS_START_TIME);
                if (!start) {
                    gen.writeNumberField("process.finish.time", time);
                }
                addField(gen, "process.variables", createProcessVariables(), lengthStringMax);
                addField(gen, "process.properties", createProcessProperties(), lengthStringMax);
                addField(gen, "process.cmdline", Util.loadString(String.format("/proc/%d/cmdline", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "process.io", Util.loadString(String.format("/proc/%d/io", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "process.limits", Util.loadString(String.format("/proc/%d/limits", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "process.mounts", Util.loadString(String.format("/proc/%d/mounts", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "process.net.dev", Util.loadString(String.format("/proc/%d/net/dev", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "process.net.protocols", Util.loadString(String.format("/proc/%d/net/protocols", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "host.name", ElasticSearchAppender.HOST_NAME, lengthStringMax);
                addField(gen, "host.ip", ElasticSearchAppender.HOST_IP, lengthStringMax);
                for (Map.Entry<String, String> e : ElasticSearchAppender.LOG_TAGS.entrySet()) {
                    addField(gen, e.getKey(), e.getValue(), lengthStringMax);
                }
                gen.writeStringField("logger", ElasticSearchAppender.class.getName());
                gen.writeNumberField("thread.id", t.getId());
                gen.writeStringField("thread.uuid", InputLogEvent.THREAD_UUID.get());
                addField(gen, "thread.name", t.getName(), lengthStringMax);
                gen.writeNumberField("thread.priority", t.getPriority());
                gen.writeNumberField("thread.count.live", THREAD_COUNT_LIVE.get());
                gen.writeNumberField("thread.count.daemon", THREAD_COUNT_DAEMON.get());
                gen.writeNumberField("thread.count.peak", THREAD_COUNT_PEAK.get());
                gen.writeNumberField("thread.count.total", THREAD_MX_BEAN.getTotalStartedThreadCount());
                gen.writeNumberField("memory.heap.init", MEMORY_HEAP_INIT.get());
                gen.writeNumberField("memory.heap.used", MEMORY_HEAP_USED.get());
                gen.writeNumberField("memory.heap.committed", MEMORY_HEAP_COMMITTED.get());
                gen.writeNumberField("memory.heap.max", MEMORY_HEAP_MAX.get());
                gen.writeNumberField("memory.non.heap.init", MEMORY_NON_HEAP_INIT.get());
                gen.writeNumberField("memory.non.heap.used", MEMORY_NON_HEAP_USED.get());
                gen.writeNumberField("memory.non.heap.committed", MEMORY_NON_HEAP_COMMITTED.get());
                gen.writeNumberField("memory.non.heap.max", MEMORY_NON_HEAP_MAX.get());
                gen.writeNumberField("memory.object.pending.finalization.count", MEMORY_OBJECT_PENDING_FINALIZATION_COUNT.get());
                gen.writeNumberField("class.count.active", CLASS_COUNT_ACTIVE.get());
                gen.writeNumberField("class.count.loaded", CLASS_LOADING_MX_BEAN.getTotalLoadedClassCount());
                gen.writeNumberField("class.count.unloaded", CLASS_LOADING_MX_BEAN.getUnloadedClassCount());
                gen.writeNumberField("compilation.time.total", COMPILATION_MX_BEAN.getTotalCompilationTime());
                gen.writeStringField("level", "INFO");
                if (start) {
                    gen.writeStringField("message", "Hello World!");
                } else {
                    gen.writeStringField("message", "Goodbye World!");
                }
                addField(gen, "appender.name", name, lengthStringMax);
                addField(gen, "appender.url", url, lengthStringMax);
                addField(gen, "appender.index", index, lengthStringMax);
                gen.writeBooleanField("appender.enable", enable);
                gen.writeNumberField("appender.count.max", countMax);
                gen.writeNumberField("appender.size.max", sizeMax);
                gen.writeNumberField("appender.bulk.count.max", bulkCountMax);
                gen.writeNumberField("appender.bulk.size.max", bulkSizeMax);
                gen.writeNumberField("appender.delay.max", delayMax);
                gen.writeNumberField("appender.bulk.retry.count", bulkRetryCount);
                gen.writeNumberField("appender.bulk.retry.delay", bulkRetryDelay);
                gen.writeNumberField("appender.event.size.start.finish", eventSizeStartFinish);
                gen.writeNumberField("appender.event.size.default", eventSizeDefault);
                gen.writeNumberField("appender.event.size.exception", eventSizeException);
                gen.writeNumberField("appender.length.string.max", lengthStringMax);
                gen.writeBooleanField("appender.out", out);
                gen.writeBooleanField("appender.set.default.uncaught.exception.handler", setDefaultUncaughtExceptionHandler);
                gen.flush();
                this.size = this.data.size() + SIZE_OVERHEAD;
                gen.writeNumberField("size", size);
                gen.writeNumberField("total.count", totalCount.incrementAndGet());
                gen.writeNumberField("total.size", totalSize.addAndGet(size));
                gen.writeNumberField("lost.count", lostCount);
                gen.writeNumberField("lost.size", lostSize);
                gen.writeEndObject();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InputLogEvent(LogEvent event,
                         AtomicLong totalCount,
                         AtomicLong totalSize,
                         long lostCount,
                         long lostSize,
                         int eventSizeDefault,
                         int eventSizeException,
                         int lengthStringMax) {
        this.id = new UUID(MOST_SIG_BITS, EVENT_LEAST_SIG_BITS.getAndIncrement()).toString();
        this.time = event.getTimeMillis();

        try {
            Throwable ex = event.getThrown();
            this.data = new ByteArrayOutputStream((ex == null) ? eventSizeDefault : eventSizeException);
            try (SmileGenerator gen = SMILE_FACTORY.createGenerator(this.data)) {
                gen.writeStartObject();
                gen.writeNumberField("time", time);
                gen.writeStringField("type", (ex == null) ? "DEFAULT" : "EXCEPTION");
                gen.writeStringField("language", "JAVA");
                gen.writeNumberField("process.id", ElasticSearchAppender.PROCESS_ID);
                gen.writeStringField("process.uuid", InputLogEvent.PROCESS_UUID);
                gen.writeNumberField("process.start.time", ElasticSearchAppender.PROCESS_START_TIME);
                addField(gen, "host.name", ElasticSearchAppender.HOST_NAME, lengthStringMax);
                addField(gen, "host.ip", ElasticSearchAppender.HOST_IP, lengthStringMax);
                for (Map.Entry<String, String> e : ElasticSearchAppender.LOG_TAGS.entrySet()) {
                    addField(gen, e.getKey(), e.getValue(), lengthStringMax);
                }
                addField(gen, "logger", event.getLoggerName(), lengthStringMax);
                gen.writeNumberField("thread.id", event.getThreadId());
                gen.writeStringField("thread.uuid", InputLogEvent.THREAD_UUID.get());
                addField(gen, "thread.name", event.getThreadName(), lengthStringMax);
                gen.writeNumberField("thread.priority", event.getThreadPriority());
                gen.writeNumberField("thread.count.live", THREAD_COUNT_LIVE.get());
                gen.writeNumberField("thread.count.daemon", THREAD_COUNT_DAEMON.get());
                gen.writeNumberField("thread.count.peak", THREAD_COUNT_PEAK.get());
                gen.writeNumberField("memory.heap.init", MEMORY_HEAP_INIT.get());
                gen.writeNumberField("memory.heap.used", MEMORY_HEAP_USED.get());
                gen.writeNumberField("memory.heap.committed", MEMORY_HEAP_COMMITTED.get());
                gen.writeNumberField("memory.heap.max", MEMORY_HEAP_MAX.get());
                gen.writeNumberField("memory.non.heap.init", MEMORY_NON_HEAP_INIT.get());
                gen.writeNumberField("memory.non.heap.used", MEMORY_NON_HEAP_USED.get());
                gen.writeNumberField("memory.non.heap.committed", MEMORY_NON_HEAP_COMMITTED.get());
                gen.writeNumberField("memory.non.heap.max", MEMORY_NON_HEAP_MAX.get());
                gen.writeNumberField("memory.object.pending.finalization.count", MEMORY_OBJECT_PENDING_FINALIZATION_COUNT.get());
                gen.writeNumberField("class.count.active", CLASS_COUNT_ACTIVE.get());
                Level l = event.getLevel();
                if (l != null) {
                    addField(gen, "level", l.toString(), lengthStringMax);
                } else {
                    gen.writeStringField("level", "INFO");
                }
                Message m = event.getMessage();
                if (m != null) {
                    addField(gen, "message", m.getFormattedMessage(), lengthStringMax);
                }
                StackTraceElement ste = event.getSource();
                if (ste != null) {
                    addField(gen, "source.file", ste.getFileName(), lengthStringMax);
                    addField(gen, "source.class", ste.getClassName(), lengthStringMax);
                    addField(gen, "source.method", ste.getMethodName(), lengthStringMax);
                    gen.writeNumberField("source.line", ste.getLineNumber());
                }
                if (ex != null) {
                    addField(gen, "exception.class", ex.getClass().getName(), lengthStringMax);
                    addField(gen, "exception.message", ex.getMessage(), lengthStringMax);
                    try (StringBuilderWriter sbw = new StringBuilderWriter(4096)) {
                        ex.printStackTrace(new PrintWriter(sbw, false));
                        addField(gen, "exception.stacktrace", sbw.toString(), lengthStringMax);
                    }
                    Throwable[] sex = ex.getSuppressed();
                    if (sex != null) {
                        gen.writeNumberField("exception.suppressed.count", sex.length);
                    }
                    Throwable cex = ex.getCause();
                    if (cex != null) {
                        addField(gen, "exception.cause.class", cex.getClass().getName(), lengthStringMax);
                        addField(gen, "exception.cause.message", cex.getMessage(), lengthStringMax);
                    }
                }
                Marker mrk = event.getMarker();
                if (mrk != null) {
                    addField(gen, "marker.name", mrk.getName(), lengthStringMax);
                    gen.writeBooleanField("marker.parents", mrk.hasParents());
                }
                ReadOnlyStringMap ctx = event.getContextData();
                if (ctx != null) {
                    ctx.forEach((k, v) -> {
                        if ((k != null) && (v != null)) {
                            addField(gen, "context." + k, v.toString(), lengthStringMax);
                        }
                    });
                }
                gen.flush();
                this.size = this.data.size() + SIZE_OVERHEAD;
                gen.writeNumberField("size", size);
                gen.writeNumberField("total.count", totalCount.incrementAndGet());
                gen.writeNumberField("total.size", totalSize.addAndGet(size));
                gen.writeNumberField("lost.count", lostCount);
                gen.writeNumberField("lost.size", lostSize);
                gen.writeEndObject();
            }
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
                            Thread.sleep(3000L);
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

    private static void addField(SmileGenerator generator, String name, String value, int lengthMax) {
        if ((name != null) && (value != null)) {
            try {
                generator.writeStringField(name, Util.cut(value, lengthMax));
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

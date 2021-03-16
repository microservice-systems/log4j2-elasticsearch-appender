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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    private static final AtomicReference<CpuUsage> CPU_USAGE = new AtomicReference<>(new CpuUsage());
    private static final AtomicReference<MemoryUsage> MEMORY_USAGE = new AtomicReference<>(new MemoryUsage());
    private static final AtomicReference<DiskUsage> DISK_USAGE = new AtomicReference<>(new DiskUsage());
    private static final AtomicReference<ClassUsage> CLASS_USAGE = new AtomicReference<>(new ClassUsage());
    private static final AtomicReference<ThreadUsage> THREAD_USAGE = new AtomicReference<>(new ThreadUsage());
    private static final Thread MONITOR_THREAD_3;
    private static final Thread MONITOR_THREAD_10;

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
        this.time = start ? ElasticSearchAppender.PROCESS_START : System.currentTimeMillis();

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
                gen.writeStringField("platform", "JAVA");
                gen.writeNumberField("process.id", ElasticSearchAppender.PROCESS_ID);
                gen.writeStringField("process.uuid", InputLogEvent.PROCESS_UUID);
                gen.writeNumberField("process.start", ElasticSearchAppender.PROCESS_START);
                if (!start) {
                    gen.writeNumberField("process.finish", time);
                }
                addField(gen, "process.variables", createProcessVariables(), lengthStringMax);
                addField(gen, "process.properties", createProcessProperties(), lengthStringMax);
                addField(gen, "process.cmdline", Util.loadString(String.format("/proc/%d/cmdline", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "process.io", Util.loadString(String.format("/proc/%d/io", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "process.limits", Util.loadString(String.format("/proc/%d/limits", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "process.mounts", Util.loadString(String.format("/proc/%d/mounts", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "process.net.dev", Util.loadString(String.format("/proc/%d/net/dev", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "process.net.protocols", Util.loadString(String.format("/proc/%d/net/protocols", ElasticSearchAppender.PROCESS_ID), "unknown"), lengthStringMax);
                addField(gen, "host.name", ElasticSearchAppender.HOST_NAME, 256);
                addField(gen, "host.ip", ElasticSearchAppender.HOST_IP, 64);
                for (Map.Entry<String, String> e : ElasticSearchAppender.LOG_TAGS.entrySet()) {
                    addField(gen, e.getKey(), e.getValue(), lengthStringMax);
                }
                gen.writeStringField("logger", ElasticSearchAppender.class.getName());
                gen.writeNumberField("thread.id", t.getId());
                gen.writeStringField("thread.uuid", InputLogEvent.THREAD_UUID.get());
                addField(gen, "thread.name", t.getName(), 256);
                gen.writeNumberField("thread.priority", t.getPriority());
                CpuUsage cpu = InputLogEvent.CPU_USAGE.get();
                gen.writeNumberField("cpu.count", cpu.count);
                gen.writeNumberField("cpu.m1", cpu.m1);
                gen.writeNumberField("cpu.m5", cpu.m5);
                gen.writeNumberField("cpu.m15", cpu.m15);
                gen.writeNumberField("cpu.entity.active", cpu.entityActive);
                gen.writeNumberField("cpu.entity.total", cpu.entityTotal);
                MemoryUsage memory = InputLogEvent.MEMORY_USAGE.get();
                gen.writeNumberField("memory.heap.init", memory.heapInit);
                gen.writeNumberField("memory.heap.used", memory.heapUsed);
                gen.writeNumberField("memory.heap.committed", memory.heapCommitted);
                gen.writeNumberField("memory.heap.max", memory.heapMax);
                gen.writeNumberField("memory.nonheap.init", memory.nonHeapInit);
                gen.writeNumberField("memory.nonheap.used", memory.nonHeapUsed);
                gen.writeNumberField("memory.nonheap.committed", memory.nonHeapCommitted);
                gen.writeNumberField("memory.nonheap.max", memory.nonHeapMax);
                gen.writeNumberField("memory.object.pending.finalization", memory.objectPendingFinalization);
                DiskUsage disk = InputLogEvent.DISK_USAGE.get();
                gen.writeNumberField("disk.total", disk.total);
                gen.writeNumberField("disk.free", disk.free);
                gen.writeNumberField("disk.usable", disk.usable);
                ClassUsage clazz = InputLogEvent.CLASS_USAGE.get();
                gen.writeNumberField("class.active", clazz.active);
                gen.writeNumberField("class.loaded", clazz.loaded);
                gen.writeNumberField("class.unloaded", clazz.unloaded);
                ThreadUsage thread = InputLogEvent.THREAD_USAGE.get();
                gen.writeNumberField("thread.live", thread.live);
                gen.writeNumberField("thread.daemon", thread.daemon);
                gen.writeNumberField("thread.peak", thread.peak);
                gen.writeNumberField("thread.total", thread.total);
                gen.writeStringField("level", "INFO");
                if (start) {
                    gen.writeStringField("message", "Hello World!");
                } else {
                    gen.writeStringField("message", "Goodbye World!");
                }
                addField(gen, "appender.name", name, 256);
                addField(gen, "appender.url", url, lengthStringMax);
                addField(gen, "appender.index", index, 256);
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
                gen.writeStringField("platform", "JAVA");
                gen.writeNumberField("process.id", ElasticSearchAppender.PROCESS_ID);
                gen.writeStringField("process.uuid", InputLogEvent.PROCESS_UUID);
                gen.writeNumberField("process.start", ElasticSearchAppender.PROCESS_START);
                addField(gen, "host.name", ElasticSearchAppender.HOST_NAME, 256);
                addField(gen, "host.ip", ElasticSearchAppender.HOST_IP, 64);
                for (Map.Entry<String, String> e : ElasticSearchAppender.LOG_TAGS.entrySet()) {
                    addField(gen, e.getKey(), e.getValue(), lengthStringMax);
                }
                addField(gen, "logger", event.getLoggerName(), 256);
                gen.writeNumberField("thread.id", event.getThreadId());
                gen.writeStringField("thread.uuid", InputLogEvent.THREAD_UUID.get());
                addField(gen, "thread.name", event.getThreadName(), 256);
                gen.writeNumberField("thread.priority", event.getThreadPriority());
                CpuUsage cpu = InputLogEvent.CPU_USAGE.get();
                gen.writeNumberField("cpu.count", cpu.count);
                gen.writeNumberField("cpu.m1", cpu.m1);
                gen.writeNumberField("cpu.m5", cpu.m5);
                gen.writeNumberField("cpu.m15", cpu.m15);
                gen.writeNumberField("cpu.entity.active", cpu.entityActive);
                gen.writeNumberField("cpu.entity.total", cpu.entityTotal);
                MemoryUsage memory = InputLogEvent.MEMORY_USAGE.get();
                gen.writeNumberField("memory.heap.init", memory.heapInit);
                gen.writeNumberField("memory.heap.used", memory.heapUsed);
                gen.writeNumberField("memory.heap.committed", memory.heapCommitted);
                gen.writeNumberField("memory.heap.max", memory.heapMax);
                gen.writeNumberField("memory.nonheap.init", memory.nonHeapInit);
                gen.writeNumberField("memory.nonheap.used", memory.nonHeapUsed);
                gen.writeNumberField("memory.nonheap.committed", memory.nonHeapCommitted);
                gen.writeNumberField("memory.nonheap.max", memory.nonHeapMax);
                gen.writeNumberField("memory.object.pending.finalization", memory.objectPendingFinalization);
                DiskUsage disk = InputLogEvent.DISK_USAGE.get();
                gen.writeNumberField("disk.total", disk.total);
                gen.writeNumberField("disk.free", disk.free);
                gen.writeNumberField("disk.usable", disk.usable);
                ClassUsage clazz = InputLogEvent.CLASS_USAGE.get();
                gen.writeNumberField("class.active", clazz.active);
                gen.writeNumberField("class.loaded", clazz.loaded);
                gen.writeNumberField("class.unloaded", clazz.unloaded);
                ThreadUsage thread = InputLogEvent.THREAD_USAGE.get();
                gen.writeNumberField("thread.live", thread.live);
                gen.writeNumberField("thread.daemon", thread.daemon);
                gen.writeNumberField("thread.peak", thread.peak);
                gen.writeNumberField("thread.total", thread.total);
                Level l = event.getLevel();
                if (l != null) {
                    addField(gen, "level", l.toString(), 256);
                } else {
                    gen.writeStringField("level", "INFO");
                }
                Message m = event.getMessage();
                if (m != null) {
                    addField(gen, "message", m.getFormattedMessage(), lengthStringMax);
                }
                StackTraceElement ste = event.getSource();
                if (ste != null) {
                    addField(gen, "source.file", ste.getFileName(), 512);
                    addField(gen, "source.class", ste.getClassName(), 512);
                    addField(gen, "source.method", ste.getMethodName(), 512);
                    gen.writeNumberField("source.line", ste.getLineNumber());
                }
                if (ex != null) {
                    addField(gen, "exception.class", ex.getClass().getName(), 512);
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
                        addField(gen, "exception.cause.class", cex.getClass().getName(), 512);
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
        MONITOR_THREAD_3 = new Thread("log4j2-elasticsearch-appender-monitor-3") {
            @Override
            public void run() {
                while (true) {
                    try {
                        MEMORY_USAGE.set(new MemoryUsage());
                        CLASS_USAGE.set(new ClassUsage());
                        THREAD_USAGE.set(new ThreadUsage());
                        try {
                            Thread.sleep(3000L);
                        } catch (InterruptedException e) {
                        }
                    } catch (Throwable e) {
                    }
                }
            }
        };
        MONITOR_THREAD_3.setDaemon(true);
        MONITOR_THREAD_3.start();
        MONITOR_THREAD_10 = new Thread("log4j2-elasticsearch-appender-monitor-10") {
            @Override
            public void run() {
                while (true) {
                    try {
                        CPU_USAGE.set(new CpuUsage());
                        DISK_USAGE.set(new DiskUsage());
                        try {
                            Thread.sleep(10000L);
                        } catch (InterruptedException e) {
                        }
                    } catch (Throwable e) {
                    }
                }
            }
        };
        MONITOR_THREAD_10.setDaemon(true);
        MONITOR_THREAD_10.start();
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

    private static final class CpuUsage {
        private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

        public final int count;
        public final float m1;
        public final float m5;
        public final float m15;
        public final int entityActive;
        public final int entityTotal;

        public CpuUsage() {
            String avg = Util.loadString("/proc/loadavg", "0.0 0.0 0.0 0/0 0");
            String[] avgs = avg.split(" ");
            String[] ents = avgs[3].split("/");
            this.count = CPU_COUNT;
            this.m1 = Float.parseFloat(avgs[0]);
            this.m5 = Float.parseFloat(avgs[1]);
            this.m15 = Float.parseFloat(avgs[2]);
            this.entityActive = Integer.parseInt(ents[0]);
            this.entityTotal = Integer.parseInt(ents[1]);
        }
    }

    private static final class MemoryUsage {
        private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

        public final long heapInit;
        public final long heapUsed;
        public final long heapCommitted;
        public final long heapMax;
        public final long nonHeapInit;
        public final long nonHeapUsed;
        public final long nonHeapCommitted;
        public final long nonHeapMax;
        public final int objectPendingFinalization;

        public MemoryUsage() {
            java.lang.management.MemoryUsage hmu = MEMORY_MX_BEAN.getHeapMemoryUsage();
            java.lang.management.MemoryUsage nhmu = MEMORY_MX_BEAN.getNonHeapMemoryUsage();

            this.heapInit = hmu.getInit() / MB;
            this.heapUsed = hmu.getUsed() / MB;
            this.heapCommitted = hmu.getCommitted() / MB;
            this.heapMax = hmu.getMax() / MB;
            this.nonHeapInit = nhmu.getInit() / MB;
            this.nonHeapUsed = nhmu.getUsed() / MB;
            this.nonHeapCommitted = nhmu.getCommitted() / MB;
            this.nonHeapMax = nhmu.getMax() / MB;
            this.objectPendingFinalization = MEMORY_MX_BEAN.getObjectPendingFinalizationCount();
        }
    }

    private static final class DiskUsage {
        private static final File ROOT_DISK = new File("/");

        public final long total;
        public final long free;
        public final long usable;

        public DiskUsage() {
            this.total = ROOT_DISK.getTotalSpace() / MB;
            this.free = ROOT_DISK.getFreeSpace() / MB;
            this.usable = ROOT_DISK.getUsableSpace() / MB;
        }
    }

    private static final class ClassUsage {
        private static final ClassLoadingMXBean CLASS_LOADING_MX_BEAN = ManagementFactory.getClassLoadingMXBean();

        public final int active;
        public final long loaded;
        public final long unloaded;

        public ClassUsage() {
            this.active = CLASS_LOADING_MX_BEAN.getLoadedClassCount();
            this.loaded = CLASS_LOADING_MX_BEAN.getTotalLoadedClassCount();
            this.unloaded = CLASS_LOADING_MX_BEAN.getUnloadedClassCount();
        }
    }

    private static final class ThreadUsage {
        private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

        public final int live;
        public final int daemon;
        public final int peak;
        public final long total;

        public ThreadUsage() {
            this.live = THREAD_MX_BEAN.getThreadCount();
            this.daemon = THREAD_MX_BEAN.getDaemonThreadCount();
            this.peak = THREAD_MX_BEAN.getPeakThreadCount();
            this.total = THREAD_MX_BEAN.getTotalStartedThreadCount();
        }
    }
}

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
import java.security.SecureRandom;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
final class InputLogEvent extends UpdateRequest implements Comparable<InputLogEvent> {
    private static final long mostSigBits = new SecureRandom().nextLong();
    private static final AtomicLong leastSigBits = new AtomicLong(0L);

    public final long time;

    public InputLogEvent(boolean start) {
        super(null, new UUID(mostSigBits, leastSigBits.incrementAndGet()).toString());

        this.time = start ? ElasticSearchAppender.PROCESS_START_TIME : System.currentTimeMillis();
        this.docAsUpsert(true);

        try {
            Thread t = Thread.currentThread();
            XContentBuilder cb = XContentFactory.smileBuilder(new ByteArrayOutputStream(131072));
            cb.humanReadable(true);
            cb.startObject();
            cb.timeField("time", time);
            if (start) {
                cb.field("type", "START");
            } else {
                cb.field("type", "FINISH");
            }
            cb.field("process.id", ElasticSearchAppender.PROCESS_ID);
            cb.timeField("process.start.time", ElasticSearchAppender.PROCESS_START_TIME);
            if (!start) {
                cb.timeField("process.finish.time", time);
            }
            addField(cb, "process.cmdline", Util.loadString(String.format("/proc/%d/cmdline", ElasticSearchAppender.PROCESS_ID), "unknown"), 65536);
            addField(cb, "process.io", Util.loadString(String.format("/proc/%d/io", ElasticSearchAppender.PROCESS_ID), "unknown"), 65536);
            addField(cb, "process.limits", Util.loadString(String.format("/proc/%d/limits", ElasticSearchAppender.PROCESS_ID), "unknown"), 65536);
            addField(cb, "process.mounts", Util.loadString(String.format("/proc/%d/mounts", ElasticSearchAppender.PROCESS_ID), "unknown"), 65536);
            addField(cb, "process.net.dev", Util.loadString(String.format("/proc/%d/net/dev", ElasticSearchAppender.PROCESS_ID), "unknown"), 65536);
            addField(cb, "process.net.protocols", Util.loadString(String.format("/proc/%d/net/protocols", ElasticSearchAppender.PROCESS_ID), "unknown"), 65536);
            for (Map.Entry<String, String> e : ElasticSearchAppender.LOG_TAGS.entrySet()) {
                cb.field(e.getKey(), e.getValue());
            }
            cb.field("host.name", ElasticSearchAppender.HOST_NAME);
            cb.field("host.ip", ElasticSearchAppender.HOST_IP);
            cb.field("logger", ElasticSearchAppender.class.getName());
            cb.field("thread.id", t.getId());
            addField(cb, "thread.name", t.getName(), 256);
            cb.field("thread.priority", t.getPriority());
            cb.field("level", "INFO");
            if (start) {
                cb.field("message", "Hello World!");
            } else {
                cb.field("message", "Goodbye World!");
            }
            cb.field("variables", ElasticSearchAppender.ENVIRONMENT_VARIABLES);
            cb.field("properties", ElasticSearchAppender.SYSTEM_PROPERTIES);
            cb.endObject();
            this.doc(cb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InputLogEvent(long lost, long lostSinceTime, long lostToTime) {
        super(null, new UUID(mostSigBits, leastSigBits.incrementAndGet()).toString());

        this.time = System.currentTimeMillis();
        this.docAsUpsert(true);

        try {
            Thread t = Thread.currentThread();
            XContentBuilder cb = XContentFactory.smileBuilder(new ByteArrayOutputStream(256));
            cb.humanReadable(true);
            cb.startObject();
            cb.timeField("time", time);
            cb.field("type", "LOST");
            cb.field("process.id", ElasticSearchAppender.PROCESS_ID);
            cb.timeField("process.start.time", ElasticSearchAppender.PROCESS_START_TIME);
            for (Map.Entry<String, String> e : ElasticSearchAppender.LOG_TAGS.entrySet()) {
                cb.field(e.getKey(), e.getValue());
            }
            cb.field("host.name", ElasticSearchAppender.HOST_NAME);
            cb.field("host.ip", ElasticSearchAppender.HOST_IP);
            cb.field("logger", ElasticSearchAppender.class.getName());
            cb.field("thread.id", t.getId());
            addField(cb, "thread.name", t.getName(), 256);
            cb.field("thread.priority", t.getPriority());
            cb.field("level", "INFO");
            cb.field("message", String.format("Lost %d events", lost));
            cb.field("lost.count", lost);
            cb.timeField("lost.since.time", lostSinceTime);
            cb.timeField("lost.to.time", lostToTime);
            cb.endObject();
            this.doc(cb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InputLogEvent(LogEvent event, int lengthMax) {
        super(null, new UUID(mostSigBits, leastSigBits.incrementAndGet()).toString());

        this.time = event.getTimeMillis();
        this.docAsUpsert(true);

        try {
            Throwable ex = event.getThrown();
            XContentBuilder cb = XContentFactory.smileBuilder(new ByteArrayOutputStream((ex == null) ? 512 : 1024));
            cb.humanReadable(true);
            cb.startObject();
            cb.timeField("time", time);
            cb.field("type", (ex == null) ? "SIMPLE" : "EXCEPTION");
            cb.field("process.id", ElasticSearchAppender.PROCESS_ID);
            cb.timeField("process.start.time", ElasticSearchAppender.PROCESS_START_TIME);
            for (Map.Entry<String, String> e : ElasticSearchAppender.LOG_TAGS.entrySet()) {
                cb.field(e.getKey(), e.getValue());
            }
            cb.field("host.name", ElasticSearchAppender.HOST_NAME);
            cb.field("host.ip", ElasticSearchAppender.HOST_IP);
            addField(cb, "logger", event.getLoggerFqcn(), 256);
            cb.field("thread.id", event.getThreadId());
            addField(cb, "thread.name", event.getThreadName(), 256);
            cb.field("thread.priority", event.getThreadPriority());
            Level l = event.getLevel();
            if (l != null) {
                addField(cb, "level", l.toString(), 32);
            } else {
                cb.field("level", "INFO");
            }
            Message m = event.getMessage();
            if (m != null) {
                addField(cb, "message", m.getFormattedMessage(), lengthMax);
            }
            StackTraceElement ste = event.getSource();
            if (ste != null) {
                addField(cb, "src.file", ste.getFileName(), 256);
                addField(cb, "src.class", ste.getClassName(), 256);
                addField(cb, "src.method", ste.getMethodName(), 256);
                cb.field("src.line", ste.getLineNumber());
            }
            if (ex != null) {
                addField(cb, "exception.class", ex.getClass().getName(), 256);
                addField(cb, "exception.message", ex.getMessage(), lengthMax);
                try (StringBuilderWriter sbw = new StringBuilderWriter(1024)) {
                    ex.printStackTrace(new PrintWriter(sbw, false));
                    addField(cb, "exception.stacktrace", sbw.toString(), lengthMax);
                }
                Throwable[] sex = ex.getSuppressed();
                if (sex != null) {
                    cb.field("exception.suppressed.count", sex.length);
                }
                Throwable cex = ex.getCause();
                if (cex != null) {
                    addField(cb, "exception.cause.class", cex.getClass().getName(), 256);
                    addField(cb, "exception.cause.message", cex.getMessage(), lengthMax);
                }
            }
            Marker mrk = event.getMarker();
            if (mrk != null) {
                addField(cb, "marker.name", mrk.getName(), 256);
                cb.field("marker.parents", mrk.hasParents());
            }
            ReadOnlyStringMap ctx = event.getContextData();
            if (ctx != null) {
                try {
                    ctx.forEach((k, v) -> {
                        if ((k != null) && (v != null)) {
                            addField(cb, "ctx." + k, v.toString(), 256);
                        }
                    });
                } catch (Exception e) {
                    addField(cb, "ctx.exception.class", e.getClass().getName(), 256);
                    addField(cb, "ctx.exception.message", e.getMessage(), lengthMax);
                }
            }
            cb.endObject();
            this.doc(cb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long estimatedSizeInBytes() {
        long s = 0L;
        if (doc() != null) {
            s += doc().source().length();
        }
        if (upsertRequest() != null) {
            s += upsertRequest().source().length();
        }
        if (script() != null) {
            s += script().getIdOrCode().length() * 2;
        }
        return s;
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

    private static void addField(XContentBuilder builder, String name, String value, int lengthMax) {
        if ((name != null) && (value != null)) {
            try {
                builder.field(name, Util.cut(value, lengthMax));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

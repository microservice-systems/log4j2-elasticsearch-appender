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

    public final long timestamp;

    public InputLogEvent(boolean start) {
        super(null, new UUID(mostSigBits, leastSigBits.incrementAndGet()).toString());

        this.timestamp = start ? ElasticSearchAppender.START : System.currentTimeMillis();
        this.docAsUpsert(true);

        try {
            Thread t = Thread.currentThread();
            XContentBuilder cb = XContentFactory.smileBuilder(new ByteArrayOutputStream(8192));
            cb.startObject();
            cb.timeField("timestamp", "time", timestamp);
            cb.timeField("start.timestamp", "start.time", ElasticSearchAppender.START);
            if (start) {
                cb.field("start", true);
            } else {
                cb.timeField("finish.timestamp", "finish.time", timestamp);
                cb.field("finish", true);
            }
            cb.field("process", ElasticSearchAppender.PROCESS);
            for (Map.Entry<String, String> e : ElasticSearchAppender.LOGTAGS.entrySet()) {
                cb.field(e.getKey(), e.getValue());
            }
            cb.field("host.name", ElasticSearchAppender.HOST_NAME);
            cb.field("host.ip", ElasticSearchAppender.HOST_IP);
            cb.field("logger", ElasticSearchAppender.class.getName());
            cb.field("thread.id", t.getId());
            addField(cb, "thread.name", t.getName(), 256);
            cb.field("thread.priority", t.getPriority());
            cb.field("level", "INFO");
            cb.field("message", "Hello World!");
            cb.field("variables", ElasticSearchAppender.VARIABLES);
            cb.field("properties", ElasticSearchAppender.PROPERTIES);
            cb.endObject();
            this.doc(cb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InputLogEvent(long lost, long lostSince, long lostTo) {
        super(null, new UUID(mostSigBits, leastSigBits.incrementAndGet()).toString());

        this.timestamp = System.currentTimeMillis();
        this.docAsUpsert(true);

        try {
            Thread t = Thread.currentThread();
            XContentBuilder cb = XContentFactory.smileBuilder(new ByteArrayOutputStream(256));
            cb.startObject();
            cb.timeField("timestamp", "time", timestamp);
            cb.timeField("start.timestamp", "start.time", ElasticSearchAppender.START);
            cb.field("process", ElasticSearchAppender.PROCESS);
            for (Map.Entry<String, String> e : ElasticSearchAppender.LOGTAGS.entrySet()) {
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
            cb.field("lost", true);
            cb.field("lost.count", lost);
            cb.timeField("lost.since.timestamp", "lost.since.time", lostSince);
            cb.timeField("lost.to.timestamp", "lost.to.time", lostTo);
            cb.endObject();
            this.doc(cb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InputLogEvent(LogEvent event, int lengthMax) {
        super(null, new UUID(mostSigBits, leastSigBits.incrementAndGet()).toString());

        this.timestamp = event.getTimeMillis();
        this.docAsUpsert(true);

        try {
            XContentBuilder cb = XContentFactory.smileBuilder(new ByteArrayOutputStream((event.getThrown() == null) ? 512 : 1024));
            cb.startObject();
            cb.timeField("timestamp", "time", timestamp);
            cb.timeField("start.timestamp", "start.time", ElasticSearchAppender.START);
            cb.field("process", ElasticSearchAppender.PROCESS);
            for (Map.Entry<String, String> e : ElasticSearchAppender.LOGTAGS.entrySet()) {
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
                cb.field("src", true);
                addField(cb, "src.file", ste.getFileName(), 256);
                addField(cb, "src.class", ste.getClassName(), 256);
                addField(cb, "src.method", ste.getMethodName(), 256);
                cb.field("src.line", ste.getLineNumber());
            }
            Throwable ex = event.getThrown();
            if (ex != null) {
                cb.field("exception", true);
                addField(cb, "exception.class", ex.getClass().getName(), 256);
                addField(cb, "exception.message", ex.getMessage(), lengthMax);
                try (StringBuilderWriter sbw = new StringBuilderWriter(1024)) {
                    ex.printStackTrace(new PrintWriter(sbw, false));
                    addField(cb, "exception.stacktrace", sbw.toString(), lengthMax);
                }
                Throwable[] sex = ex.getSuppressed();
                if (sex != null) {
                    cb.field("exception.suppressed", true);
                    cb.field("exception.suppressed.count", sex.length);
                }
                Throwable cex = ex.getCause();
                if (cex != null) {
                    cb.field("exception.cause", true);
                    addField(cb, "exception.cause.class", cex.getClass().getName(), 256);
                    addField(cb, "exception.cause.message", cex.getMessage(), lengthMax);
                }
            }
            Marker mrk = event.getMarker();
            if (mrk != null) {
                cb.field("marker", true);
                addField(cb, "marker.name", mrk.getName(), 256);
                cb.field("marker.parents", mrk.hasParents());
            }
            ReadOnlyStringMap ctx = event.getContextData();
            if (ctx != null) {
                cb.field("ctx", true);
                try {
                    ctx.forEach((k, v) -> {
                        if ((k != null) && (v != null)) {
                            addField(cb, "ctx." + k, v.toString(), 256);
                        }
                    });
                } catch (Exception e) {
                    cb.field("ctx.exception", true);
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
        if (timestamp < event.timestamp) {
            return -1;
        } else if (timestamp > event.timestamp) {
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

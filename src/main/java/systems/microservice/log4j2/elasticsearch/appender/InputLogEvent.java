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

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.util.ReadOnlyStringMap;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import systems.microservice.log4j2.elasticsearch.appender.util.string.StringUtil;

import java.io.IOException;
import java.security.SecureRandom;
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

    public InputLogEvent(String index, LogEvent event, int length) {
        super(index, new UUID(mostSigBits, leastSigBits.incrementAndGet()).toString());

        this.timestamp = event.getTimeMillis();
        this.docAsUpsert(true);

        try {
            XContentBuilder cb = XContentFactory.smileBuilder();
            cb.startObject();
            cb.timeField("millis", "timestamp", timestamp);
            addField(cb, "logger", event.getLoggerFqcn(), 256);
            cb.field("thread_id", event.getThreadId());
            addField(cb, "thread_name", event.getThreadName(), 256);
            cb.field("thread_priority", event.getThreadPriority());
            addField(cb, "level", event.getLevel().toString(), 32);
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
                addField(cb, "exception.message", ex.getMessage(), length);
                Throwable cex = ex.getCause();
                if (cex != null) {
                    cb.field("exception.cause", true);
                    addField(cb, "exception.cause.class", cex.getClass().getName(), 256);
                    addField(cb, "exception.cause.message", cex.getMessage(), length);
                }
            }
            Marker m = event.getMarker();
            if (m != null) {
                cb.field("marker", true);
                addField(cb, "marker.name", m.getName(), 256);
                cb.field("marker.parents", m.hasParents());
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
                    addField(cb, "ctx.exception.message", e.getMessage(), length);
                }
            }
            addField(cb, "message", event.getMessage().getFormattedMessage(), length);
            cb.endObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    private static void addField(XContentBuilder builder, String name, String value, int length) {
        if (value != null) {
            try {
                builder.field(name, StringUtil.cut(value, length));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

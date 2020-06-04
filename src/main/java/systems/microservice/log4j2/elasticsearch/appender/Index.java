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

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
final class Index {
    public static final long DAY_MILLIS = 24L * 60L * 60L * 1000L;

    public final String prefix;
    public final String name;
    public final long timeBegin;
    public final long timeEnd;

    public Index(String prefix, InputLogEvent event) {
        long tb = createTimeBegin(event.time);

        this.prefix = prefix;
        this.name = String.format("%s-%s", prefix, String.format("%1$tY.%1$tm.%1$td", tb));
        this.timeBegin = tb;
        this.timeEnd = tb + DAY_MILLIS;
    }

    public boolean contains(InputLogEvent event) {
        return (event.time >= timeBegin) && (event.time < timeEnd);
    }

    private static long createTimeBegin(long time) {
        long d = time / DAY_MILLIS;
        return d * DAY_MILLIS;
    }
}

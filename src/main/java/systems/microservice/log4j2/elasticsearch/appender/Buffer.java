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

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
final class Buffer {
    public static final int BULK_RETRIES = 5;
    public static final long BULK_RETRIES_SPAN = 5000L;
    public static final int BULK_MAX_COUNT = 10000;
    public static final long BULK_MAX_SIZE = 1048576L;

    private final ThreadSection section = new ThreadSection(true);
    private final AtomicInteger size = new AtomicInteger(0);
    private final int capacity;
    private final ConcurrentLinkedQueue<InputLogEvent> eventsQueue;
    private final ArrayList<InputLogEvent> eventsList;

    public Buffer(int capacity) {
        this.capacity = capacity;
        this.eventsQueue = new ConcurrentLinkedQueue<>();
        this.eventsList = new ArrayList<>(capacity + 1);
    }

    public boolean isReady() {
        return section.isEnabled();
    }

    public boolean append(InputLogEvent event, Thread flushThread) {
        if (section.enter()) {
            try {
                if (size.get() < capacity) {
                    int s = size.getAndIncrement();
                    if (s < capacity) {
                        eventsQueue.offer(event);
                        if (s + 1 == capacity) {
                            section.disable();
                            flushThread.interrupt();
                        }
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } finally {
                section.leave();
            }
        } else {
            return false;
        }
    }

    public void flush(RestHighLevelClient client, String group, AtomicLong lost, AtomicLong lostSince) {
        section.disable();
        try {
            section.await(500L);
            if (size.get() > 0) {
                try {
                    for (InputLogEvent e : eventsQueue) {
                        eventsList.add(e);
                    }
                    InputLogEvent le = null;
                    long l = lost.get();
                    if (l > 0L) {
                        le = new InputLogEvent(l, lostSince.get(), System.currentTimeMillis());
                        eventsList.add(le);
                    }
                    Collections.sort(eventsList);
                    boolean lef = false;
                    BulkRequest r = new BulkRequest(group);
                    for (InputLogEvent e : eventsList) {
                        if ((r.numberOfActions() < BULK_MAX_COUNT) && (r.estimatedSizeInBytes() < BULK_MAX_SIZE)) {
                            r.add(e);
                            if (e == le) {
                                lef = true;
                            }
                        } else {
                            if (putEvents(client, group, r)) {
                                if (lef) {
                                    lost.addAndGet(-l);
                                }
                            } else {
                                lost.addAndGet(r.numberOfActions());
                            }
                            lef = false;
                            r = new BulkRequest(group);
                            r.add(e);
                            if (e == le) {
                                lef = true;
                            }
                        }
                    }
                    if (putEvents(client, group, r)) {
                        if (lef) {
                            lost.addAndGet(-l);
                        }
                    } else {
                        lost.addAndGet(r.numberOfActions());
                    }
                } finally {
                    eventsList.clear();
                    eventsQueue.clear();
                    size.set(0);
                }
            }
        } finally {
            section.enable();
        }
    }

    private boolean putEvents(RestHighLevelClient client, String group, BulkRequest request) {
        if (request.numberOfActions() > 0) {
            for (int i = 0; i < BULK_RETRIES; ++i) {
                try {
                    client.bulk(request, RequestOptions.DEFAULT);
                    return true;
                } catch (Exception e) {
                }
                try {
                    Thread.sleep(BULK_RETRIES_SPAN);
                } catch (InterruptedException ex) {
                }
            }
            return false;
        } else {
            return true;
        }
    }
}

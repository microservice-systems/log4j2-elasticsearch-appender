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
    public static final int BULK_RETRIES = 10;
    public static final long BULK_RETRIES_SPAN = 3000L;
    public static final int BULK_MAX_COUNT = 10000;
    public static final long BULK_MAX_SIZE = 1048576L;

    private final AtomicBoolean ready = new AtomicBoolean(true);
    private final AtomicInteger threads = new AtomicInteger(0);
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
        return ready.get();
    }

    public boolean append(InputLogEvent event, FlushWait flushWait) {
        if (ready.get()) {
            threads.incrementAndGet();
            try {
                if (ready.get()) {
                    if (size.get() < capacity) {
                        int s = size.getAndIncrement();
                        if (s < capacity) {
                            eventsQueue.offer(event);
                            if (s + 1 == capacity) {
                                flushWait.signalAll(new Runnable() {
                                    @Override
                                    public void run() {
                                        ready.set(false);
                                    }
                                });
                            }
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } finally {
                threads.decrementAndGet();
            }
        } else {
            return false;
        }
    }

    public void flush(RestHighLevelClient client, String group, AtomicLong lost, AtomicLong lostSince) {
        ready.set(false);
        try {
            while (threads.get() > 0) {
                try {
                    Thread.sleep(500L);
                } catch (InterruptedException e) {
                }
            }
            if (size.get() > 0) {
                try {
                    for (InputLogEvent e : eventsQueue) {
                        eventsList.add(e);
                    }
                    long l = lost.getAndSet(0L);
                    if (l > 0L) {
                        InputLogEvent e = new InputLogEvent(l, lostSince.get(), System.currentTimeMillis());
                        eventsList.add(e);
                    }
                    Collections.sort(eventsList);
                    BulkRequest r = new BulkRequest(group);
                    for (InputLogEvent e : eventsList) {
                        if ((r.numberOfActions() < BULK_MAX_COUNT) && (r.estimatedSizeInBytes() < BULK_MAX_SIZE)) {
                            r.add(e);
                        } else {
                            putEvents(client, group, lost, r);
                            r = new BulkRequest(group);
                            r.add(e);
                        }
                    }
                    putEvents(client, group, lost, r);
                } finally {
                    eventsList.clear();
                    eventsQueue.clear();
                    size.set(0);
                }
            }
        } finally {
            ready.set(true);
        }
    }

    private void putEvents(RestHighLevelClient client, String group, AtomicLong lost, BulkRequest request) {
        int c = request.numberOfActions();
        if (c > 0) {
            int i = 0;
            for (; i < BULK_RETRIES; ++i) {
                try {
                    client.bulk(request, RequestOptions.DEFAULT);
                } catch (Exception e) {
                    try {
                        Thread.sleep(BULK_RETRIES_SPAN);
                    } catch (InterruptedException ex) {
                    }
                    continue;
                }
                break;
            }
            if (i >= BULK_RETRIES) {
                lost.addAndGet(c);
            }
        }
    }
}

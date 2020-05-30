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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
final class Buffer {
    public static final int BULK_RETRIES = 5;
    public static final long BULK_RETRIES_SPAN = 5000L;
    public static final int BULK_COUNT_MAX = 10000;
    public static final long BULK_SIZE_MAX = 1048576L;

    private final ThreadSection section = new ThreadSection(true);
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicLong size = new AtomicLong(0L);
    private final int countMax;
    private final long sizeMax;
    private final ConcurrentLinkedQueue<InputLogEvent> eventsQueue;
    private final ArrayList<InputLogEvent> eventsList;

    public Buffer(int countMax, long sizeMax) {
        this.countMax = countMax;
        this.sizeMax = sizeMax;
        this.eventsQueue = new ConcurrentLinkedQueue<>();
        this.eventsList = new ArrayList<>(countMax + 1);
    }

    public boolean isReady() {
        return section.isEnabled();
    }

    public boolean append(InputLogEvent event) {
        if (section.enter()) {
            try {
                if (count.get() + 1 < countMax) {
                    long es = event.estimatedSizeInBytes();
                    if (size.get() + es < sizeMax) {
                        int c = count.incrementAndGet();
                        if (c < countMax) {
                            long s = size.addAndGet(es);
                            if (s < sizeMax) {
                                eventsQueue.offer(event);
                                return true;
                            }
                        }
                    }
                }
            } finally {
                section.leave();
            }
            section.disable();
        }
        return false;
    }

    public void flush(RestHighLevelClient client, String url, String index, AtomicLong lost, AtomicLong lostSince) {
        section.disable();
        try {
            section.await(500L);
            if (count.get() > 0) {
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
                    Index idx = null;
                    boolean lef = false;
                    BulkRequest r = new BulkRequest(null);
                    for (InputLogEvent e : eventsList) {
                        if ((idx == null) || !idx.contains(e)) {
                            idx = new Index(index, e);
                        }
                        e.index(idx.name);
                        if ((r.numberOfActions() < BULK_COUNT_MAX) && (r.estimatedSizeInBytes() < BULK_SIZE_MAX)) {
                            r.add(e);
                            if (e == le) {
                                lef = true;
                            }
                        } else {
                            if (putEvents(client, url, index, r)) {
                                if (lef) {
                                    lost.addAndGet(-l);
                                }
                            } else {
                                lost.addAndGet(r.numberOfActions());
                            }
                            lef = false;
                            r = new BulkRequest(index);
                            r.add(e);
                            if (e == le) {
                                lef = true;
                            }
                        }
                    }
                    if (putEvents(client, url, index, r)) {
                        if (lef) {
                            lost.addAndGet(-l);
                        }
                    } else {
                        lost.addAndGet(r.numberOfActions());
                    }
                } finally {
                    eventsList.clear();
                    eventsQueue.clear();
                    count.set(0);
                }
            }
        } finally {
            section.enable();
        }
    }

    private boolean putEvents(RestHighLevelClient client, String url, String index, BulkRequest request) {
        int c = request.numberOfActions();
        if (c > 0) {
            for (int i = 0; i < BULK_RETRIES; ++i) {
                try {
                    client.bulk(request, RequestOptions.DEFAULT);
                    return true;
                } catch (Exception e) {
                    ElasticSearchAppender.logSystem(Buffer.class, String.format("Attempt %d to put %d events to ElasticSearch (%s, %s) is failed: %s", i, c, url, index, e.getMessage()));
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

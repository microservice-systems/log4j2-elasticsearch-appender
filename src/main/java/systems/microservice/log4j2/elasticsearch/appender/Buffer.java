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

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
    public static final int BULK_COUNT_MAX = 4000;
    public static final long BULK_SIZE_MAX = 2097152L;

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
                    long es = event.size;
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

    public void flush(RestHighLevelClient client, String url, String index, AtomicLong lostCount, AtomicLong lostSize) {
        section.disable();
        try {
            section.await(100L);
            if (count.get() > 0) {
                try {
                    for (InputLogEvent e : eventsQueue) {
                        eventsList.add(e);
                    }
                    Collections.sort(eventsList);
                    Index idx = null;
                    BulkRequest r = new BulkRequest(null);
                    for (InputLogEvent e : eventsList) {
                        if ((idx == null) || !idx.contains(e)) {
                            idx = new Index(index, e);
                        }
                        e.index(idx.name);
                        if ((r.numberOfActions() < BULK_COUNT_MAX) && (r.estimatedSizeInBytes() < BULK_SIZE_MAX)) {
                            r.add(e);
                        } else {
                            lostCount.addAndGet(putEvents(client, url, index, r));
                            r = new BulkRequest(null);
                            r.add(e);
                        }
                    }
                    lostCount.addAndGet(putEvents(client, url, index, r));
                } finally {
                    eventsList.clear();
                    eventsQueue.clear();
                    size.set(0L);
                    count.set(0);
                }
            }
        } finally {
            section.enable();
        }
    }

    private int putEvents(RestHighLevelClient client, String url, String index, BulkRequest request) {
        int fc = 0;
        for (int i = 0; (request.numberOfActions() > 0) && (i < BULK_RETRIES); ++i) {
            try {
                BulkResponse rsp = client.bulk(request, RequestOptions.DEFAULT);
                fc = 0;
                BulkItemResponse[] irs = rsp.getItems();
                for (BulkItemResponse ir : irs) {
                    if (ir.isFailed()) {
                        fc++;
                    }
                }
                if (fc == 0) {
                    return 0;
                } else {
                    ElasticSearchAppender.logSystem(Buffer.class, String.format("Attempt %d to put %d events to ElasticSearch (%s, %s) contains %d failed events", i, request.numberOfActions(), url, index, fc));
                    HashSet<String> fids = new HashSet<>(Math.max(fc, 16));
                    for (BulkItemResponse ir : irs) {
                        fids.add(ir.getId());
                    }
                    BulkRequest r = new BulkRequest(null);
                    List<DocWriteRequest<?>> es = request.requests();
                    for (DocWriteRequest<?> e : es) {
                        if (fids.contains(e.id())) {
                            r.add(e);
                        }
                    }
                    request = r;
                }
            } catch (Exception e) {
                ElasticSearchAppender.logSystem(Buffer.class, String.format("Attempt %d to put %d events to ElasticSearch (%s, %s) is failed with %s: %s", i, request.numberOfActions(), url, index, e.getClass().getSimpleName(), e.getMessage()));
            }
            try {
                Thread.sleep(BULK_RETRIES_SPAN);
            } catch (InterruptedException ex) {
            }
        }
        return fc;
    }
}

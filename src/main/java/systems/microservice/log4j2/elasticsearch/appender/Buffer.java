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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
final class Buffer {
    private final ThreadSection section = new ThreadSection(true);
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicLong size = new AtomicLong(0L);
    private final int countMax;
    private final long sizeMax;
    private final int bulkCountMax;
    private final long bulkSizeMax;
    private final int bulkRetryCount;
    private final long bulkRetryDelay;
    private final ConcurrentLinkedQueue<InputLogEvent> eventsQueue;
    private final ArrayList<InputLogEvent> eventsList;

    public Buffer(int countMax,
                  long sizeMax,
                  int bulkCountMax,
                  long bulkSizeMax,
                  int bulkRetryCount,
                  long bulkRetryDelay) {
        this.countMax = countMax;
        this.sizeMax = sizeMax;
        this.bulkCountMax = bulkCountMax;
        this.bulkSizeMax = bulkSizeMax;
        this.bulkRetryCount = bulkRetryCount;
        this.bulkRetryDelay = bulkRetryDelay;
        this.eventsQueue = new ConcurrentLinkedQueue<>();
        this.eventsList = new ArrayList<>(countMax);
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

    public void flush(AtomicBoolean enabled,
                      RestHighLevelClient client,
                      String name,
                      String url,
                      String index,
                      AtomicLong lostCount,
                      AtomicLong lostSize,
                      boolean out) {
        section.disable();
        try {
            section.await();
            if (count.get() > 0) {
                try {
                    for (InputLogEvent e : eventsQueue) {
                        eventsList.add(e);
                    }
                    Collections.sort(eventsList);
                    Index idx = null;
                    int bc = 0;
                    long bs = 0L;
                    BulkRequest r = new BulkRequest(null);
                    for (InputLogEvent e : eventsList) {
                        if ((idx == null) || !idx.contains(e)) {
                            idx = new Index(index, e);
                        }
                        e.index(idx.name);
                        if ((bc >= bulkCountMax) || (bs >= bulkSizeMax)) {
                            putEvents(enabled, client, name, url, index, lostCount, lostSize, out, r);
                            r = new BulkRequest(null);
                            bc = 0;
                            bs = 0L;
                        }
                        r.add(e);
                        bc++;
                        bs += e.size;
                    }
                    putEvents(enabled, client, name, url, index, lostCount, lostSize, out, r);
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

    private void putEvents(AtomicBoolean enabled,
                           RestHighLevelClient client,
                           String name,
                           String url,
                           String index,
                           AtomicLong lostCount,
                           AtomicLong lostSize,
                           boolean out,
                           BulkRequest request) {
        int fc = 0;
        long fs = 0L;
        try {
            for (int i = 0; (request.numberOfActions() > 0) && (i < bulkRetryCount); ++i) {
                BulkResponse rsp = null;
                try {
                    rsp = client.bulk(request, RequestOptions.DEFAULT);
                } catch (Exception ex) {
                    fc = 0;
                    fs = 0L;
                    List<DocWriteRequest<?>> es = request.requests();
                    for (DocWriteRequest<?> e : es) {
                        if (e instanceof InputLogEvent) {
                            InputLogEvent ile = (InputLogEvent) e;
                            fc++;
                            fs += ile.size;
                        }
                    }
                    ElasticSearchAppender.logSystem(out, Buffer.class, String.format("Attempt %d to put %d events to ElasticSearch (%s, %s, %s) is failed with %s: %s", i, request.numberOfActions(), name, url, index, ex.getClass().getSimpleName(), ex.getMessage()));
                    if (Util.delay(enabled, bulkRetryDelay, 200L)) {
                        continue;
                    } else {
                        return;
                    }
                }
                fc = 0;
                fs = 0L;
                if (!rsp.hasFailures()) {
                    return;
                } else {
                    BulkItemResponse[] irs = rsp.getItems();
                    HashSet<String> fids = new HashSet<>(Math.max(fc, 16));
                    for (BulkItemResponse ir : irs) {
                        fids.add(ir.getId());
                    }
                    BulkRequest r = new BulkRequest(null);
                    List<DocWriteRequest<?>> es = request.requests();
                    for (DocWriteRequest<?> e : es) {
                        if (fids.contains(e.id())) {
                            if (e instanceof InputLogEvent) {
                                InputLogEvent ile = (InputLogEvent) e;
                                r.add(ile);
                                fc++;
                                fs += ile.size;
                            }
                        }
                    }
                    ElasticSearchAppender.logSystem(out, Buffer.class, String.format("Attempt %d to put %d events to ElasticSearch (%s, %s, %s) contains %d failed events of size %d", i, request.numberOfActions(), name, url, index, fc, fs));
                    request = r;
                }
                if (!Util.delay(enabled, bulkRetryDelay, 200L)) {
                    return;
                }
            }
        } finally {
            lostCount.addAndGet(fc);
            lostSize.addAndGet(fs);
        }
    }
}

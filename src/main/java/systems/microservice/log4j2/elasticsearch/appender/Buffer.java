/*
 * Copyright (C) 2017 Dmitry Kotlyarov.
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

package pro.apphub.aws.cloudwatch.log4j2;

import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.DataAlreadyAcceptedException;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.InvalidSequenceTokenException;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.PutLogEventsResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
final class Buffer {
    public static final int MAX_BATCH_COUNT = 10000;
    public static final int MAX_BATCH_SIZE = 1048576;

    private final AtomicBoolean ready = new AtomicBoolean(true);
    private final AtomicInteger threads = new AtomicInteger(0);
    private final AtomicInteger size = new AtomicInteger(0);
    private final int capacity;
    private final ConcurrentLinkedQueue<InputLogEvent> eventsQueue;
    private final ArrayList<InputLogEvent> eventsList;
    private final ArrayList<InputLogEvent> eventsBatch;

    public Buffer(int capacity) {
        this.capacity = capacity;
        this.eventsQueue = new ConcurrentLinkedQueue<>();
        this.eventsList = new ArrayList<>(capacity + 1);
        this.eventsBatch = new ArrayList<>(Math.min(capacity + 1, MAX_BATCH_COUNT));
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

    public FlushInfo flush(AWSLogsClient client, String group, String stream, FlushInfo info, AtomicLong lost) {
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
                        InputLogEvent e = new InputLogEvent();
                        e.setTimestamp(System.currentTimeMillis());
                        e.setMessage(String.format("[EVENTS_LOST]: %d", l));
                        eventsList.add(e);
                    }
                    Collections.sort(eventsList, new Comparator<InputLogEvent>() {
                        @Override
                        public int compare(InputLogEvent o1, InputLogEvent o2) {
                            return o1.getTimestamp().compareTo(o2.getTimestamp());
                        }
                    });
                    long lst = info.last;
                    if (lst > 0L) {
                        for (InputLogEvent e : eventsList) {
                            if (e.getTimestamp() < lst) {
                                e.setTimestamp(lst);
                            } else {
                                break;
                            }
                        }
                    }
                    lst = eventsList.get(eventsList.size() - 1).getTimestamp();
                    String tok = info.token;
                    int c = 0;
                    int s = 0;
                    for (InputLogEvent e : eventsList) {
                        int es = e.getMessage().length() * 4 + 26;
                        if ((c + 1 < MAX_BATCH_COUNT) && (s + es < MAX_BATCH_SIZE)) {
                            c++;
                            s += es;
                            eventsBatch.add(e);
                        } else {
                            tok = putEvents(client, group, stream, tok, lost, eventsBatch);
                            c = 1;
                            s = es;
                            eventsBatch.clear();
                            eventsBatch.add(e);
                        }
                    }
                    tok = putEvents(client, group, stream, tok, lost, eventsBatch);
                    return new FlushInfo(lst, tok);
                } finally {
                    eventsBatch.clear();
                    eventsList.clear();
                    eventsQueue.clear();
                    size.set(0);
                }
            } else {
                return info;
            }
        } finally {
            ready.set(true);
        }
    }

    private String putEvents(AWSLogsClient client,
                             String group,
                             String stream,
                             String token,
                             AtomicLong lost,
                             ArrayList<InputLogEvent> events) {
        if (!events.isEmpty()) {
            try {
                PutLogEventsRequest req = new PutLogEventsRequest(group, stream, events);
                req.setSequenceToken(token);
                PutLogEventsResult res = client.putLogEvents(req);
                return res.getNextSequenceToken();
            } catch (DataAlreadyAcceptedException e) {
                lost.addAndGet(events.size());
                return e.getExpectedSequenceToken();
            } catch (InvalidSequenceTokenException e) {
                lost.addAndGet(events.size());
                return e.getExpectedSequenceToken();
            } catch (Exception e) {
                lost.addAndGet(events.size());
                return token;
            }
        } else {
            return token;
        }
    }
}

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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
final class FlushWait {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final long span;
    private long prev = System.currentTimeMillis();

    public FlushWait(int span) {
        this.span = span * 1000L;
    }

    public void await(AtomicBoolean enabled, Buffer buffer1, Buffer buffer2) {
        long next = prev + span;
        long time = System.currentTimeMillis();
        if (time < next) {
            lock.lock();
            try {
                if (enabled.get() && buffer1.isReady() && buffer2.isReady()) {
                    try {
                        condition.await(next - time, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                    }
                }
            } finally {
                lock.unlock();
            }
        }
        prev = System.currentTimeMillis();
    }

    public void signalAll() {
        lock.lock();
        try {
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void signalAll(Runnable activity) {
        lock.lock();
        try {
            activity.run();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}

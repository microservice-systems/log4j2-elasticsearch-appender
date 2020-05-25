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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public final class ThreadSection {
    private final AtomicBoolean enabled;
    private final AtomicLong threads = new AtomicLong(0L);

    public ThreadSection(boolean enabled) {
        this.enabled = new AtomicBoolean(enabled);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public boolean isDisabled() {
        return !enabled.get() && (threads.get() == 0L);
    }

    public boolean enable() {
        return enabled.compareAndSet(false, true);
    }

    public boolean disable() {
        return enabled.compareAndSet(true, false);
    }

    public void await(long millis) {
        while (!isDisabled()) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
            }
        }
    }

    public boolean enter() {
        if (enabled.get()) {
            threads.incrementAndGet();
            if (enabled.get()) {
                return true;
            } else {
                threads.decrementAndGet();
                return false;
            }
        } else {
            return false;
        }
    }

    public void leave() {
        long ts = threads.decrementAndGet();
        if (ts < 0L) {
            threads.incrementAndGet();
            throw new IllegalStateException(String.format("Illegal enter/leave calls: %d", ts));
        }
    }
}

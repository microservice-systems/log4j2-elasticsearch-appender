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

package systems.microservice.log4j2.elasticsearch.appender.test4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.ArrayList;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public class Test4 {
    private static final Logger log = LogManager.getLogger(Test4.class);

    public Test4() {
    }

    @Test
    public void test() throws Exception {
        ArrayList<Thread> ts = new ArrayList<>(64);
        for (int i = 0; i < 64; ++i) {
            Thread t = new Thread(String.format("test-%d", i)) {
                @Override
                public void run() {
                    for (int j = 0; j < 16384; ++j) {
                        log.info("Hello, World {}", j);
                        try {
                            Thread.sleep(5L);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            };
            t.setDaemon(false);
            t.start();
            ts.add(t);
        }
        for (Thread t : ts) {
            t.join();
        }
    }
}

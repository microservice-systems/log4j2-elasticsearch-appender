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

import java.nio.charset.Charset;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public final class Util {
    public static final String EMPTY_STRING = "";
    public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    private Util() {
    }

    public static String cut(String value, int lengthMax) {
        if (value != null) {
            if (value.length() <= lengthMax) {
                return value;
            } else {
                return value.substring(0, lengthMax);
            }
        } else {
            return null;
        }
    }
}

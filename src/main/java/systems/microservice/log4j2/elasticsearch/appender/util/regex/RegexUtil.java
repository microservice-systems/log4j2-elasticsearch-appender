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

package systems.microservice.log4j2.elasticsearch.appender.util.regex;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
public final class RegexUtil {
    private RegexUtil() {
    }

    public static Pattern compile(String regex) {
        if (regex != null) {
            return Pattern.compile(regex);
        } else {
            return null;
        }
    }

    public static boolean match(Pattern regex, String value) {
        if (regex != null) {
            if (value != null) {
                return regex.matcher(value).matches();
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    public static List<String> match(Pattern regex, String[] values) {
        LinkedList<String> l = new LinkedList<>();
        for (String v : values) {
            if (match(regex, v)) {
                l.add(v);
            }
        }
        return l;
    }
}

/*
 * Copyright (C) 2020 Microservice Systems, Inc.
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

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;

/**
 * @author Dmitry Kotlyarov
 * @since 1.0
 */
@Plugin(name = "ElasticSearch", category = "Core", elementType = "appender", printObject = true)
public final class ElasticSearchAppender extends AbstractAppender {
    private final ElasticSearchAppenderSingleton singleton;

    public ElasticSearchAppender(String name,
                                 String url,
                                 String user,
                                 String password,
                                 String index,
                                 boolean enable,
                                 int countMax,
                                 long sizeMax,
                                 int bulkCountMax,
                                 long bulkSizeMax,
                                 long delayMax,
                                 int bulkRetryCount,
                                 long bulkRetryDelay,
                                 int eventSizeStartFinish,
                                 int eventSizeDefault,
                                 int eventSizeException,
                                 int lengthStringMax,
                                 boolean out,
                                 boolean debug,
                                 boolean setDefaultUncaughtExceptionHandler,
                                 Filter filter,
                                 Layout<? extends Serializable> layout) {
        super(name, filter, (layout != null) ? layout : PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY);

        this.singleton = ElasticSearchAppenderSingleton.getInstance(name, url, user, password, index, enable,
                                                                    countMax, sizeMax, bulkCountMax, bulkSizeMax, delayMax, bulkRetryCount, bulkRetryDelay,
                                                                    eventSizeStartFinish, eventSizeDefault, eventSizeException, lengthStringMax,
                                                                    out, debug, setDefaultUncaughtExceptionHandler);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void append(LogEvent event) {
        singleton.append(event);
    }

    @PluginFactory
    public static ElasticSearchAppender createAppender(@PluginAttribute("name") String name,
                                                       @PluginAttribute("url") String url,
                                                       @PluginAttribute("user") String user,
                                                       @PluginAttribute("password") String password,
                                                       @PluginAttribute("index") String index,
                                                       @PluginAttribute("enable") String enable,
                                                       @PluginAttribute("countMax") String countMax,
                                                       @PluginAttribute("sizeMax") String sizeMax,
                                                       @PluginAttribute("bulkCountMax") String bulkCountMax,
                                                       @PluginAttribute("bulkSizeMax") String bulkSizeMax,
                                                       @PluginAttribute("delayMax") String delayMax,
                                                       @PluginAttribute("bulkRetryCount") String bulkRetryCount,
                                                       @PluginAttribute("bulkRetryDelay") String bulkRetryDelay,
                                                       @PluginAttribute("eventSizeStartFinish") String eventSizeStartFinish,
                                                       @PluginAttribute("eventSizeDefault") String eventSizeDefault,
                                                       @PluginAttribute("eventSizeException") String eventSizeException,
                                                       @PluginAttribute("lengthStringMax") String lengthStringMax,
                                                       @PluginAttribute("out") String out,
                                                       @PluginAttribute("debug") String debug,
                                                       @PluginAttribute("setDefaultUncaughtExceptionHandler") String setDefaultUncaughtExceptionHandler,
                                                       @PluginElement("Filter") Filter filter,
                                                       @PluginElement("Layout") Layout<? extends Serializable> layout) {
        return new ElasticSearchAppender((name != null) ? name : "elasticsearch",
                                         getProperty("log4j2.elasticsearch.url", "LOG4J2_ELASTICSEARCH_URL", url, null),
                                         getProperty("log4j2.elasticsearch.user", "LOG4J2_ELASTICSEARCH_USER", user, null),
                                         getProperty("log4j2.elasticsearch.password", "LOG4J2_ELASTICSEARCH_PASSWORD", password, null),
                                         getProperty("log4j2.elasticsearch.index", "LOG4J2_ELASTICSEARCH_INDEX", index, null),
                                         Boolean.parseBoolean(getProperty("log4j2.elasticsearch.enable", "LOG4J2_ELASTICSEARCH_ENABLE", enable, "true")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.count.max", "LOG4J2_ELASTICSEARCH_COUNT_MAX", countMax, "20000")),
                                         Long.parseLong(getProperty("log4j2.elasticsearch.size.max", "LOG4J2_ELASTICSEARCH_SIZE_MAX", sizeMax, "10485760")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.bulk.count.max", "LOG4J2_ELASTICSEARCH_BULK_COUNT_MAX", bulkCountMax, "8000")),
                                         Long.parseLong(getProperty("log4j2.elasticsearch.bulk.size.max", "LOG4J2_ELASTICSEARCH_BULK_SIZE_MAX", bulkSizeMax, "4194304")),
                                         Long.parseLong(getProperty("log4j2.elasticsearch.delay.max", "LOG4J2_ELASTICSEARCH_DELAY_MAX", delayMax, "60")) * 1000L,
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.bulk.retry.count", "LOG4J2_ELASTICSEARCH_BULK_RETRY_COUNT", bulkRetryCount, "5")),
                                         Long.parseLong(getProperty("log4j2.elasticsearch.bulk.retry.delay", "LOG4J2_ELASTICSEARCH_BULK_RETRY_DELAY", bulkRetryDelay, "5")) * 1000L,
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.event.size.start.finish", "LOG4J2_ELASTICSEARCH_EVENT_SIZE_START_FINISH", eventSizeStartFinish, "65536")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.event.size.default", "LOG4J2_ELASTICSEARCH_EVENT_SIZE_DEFAULT", eventSizeDefault, "1536")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.event.size.exception", "LOG4J2_ELASTICSEARCH_EVENT_SIZE_EXCEPTION", eventSizeException, "8192")),
                                         Integer.parseInt(getProperty("log4j2.elasticsearch.length.string.max", "LOG4J2_ELASTICSEARCH_LENGTH_STRING_MAX", lengthStringMax, "65536")),
                                         Boolean.parseBoolean(getProperty("log4j2.elasticsearch.out", "LOG4J2_ELASTICSEARCH_OUT", out, "true")),
                                         Boolean.parseBoolean(getProperty("log4j2.elasticsearch.debug", "LOG4J2_ELASTICSEARCH_DEBUG", debug, "false")),
                                         Boolean.parseBoolean(getProperty("log4j2.elasticsearch.set.default.uncaught.exception.handler", "LOG4J2_ELASTICSEARCH_SET_DEFAULT_UNCAUGHT_EXCEPTION_HANDLER", setDefaultUncaughtExceptionHandler, "true")),
                                         filter,
                                         layout);
    }

    private static String getProperty(String property, String variable, String value, String def) {
        String v = System.getProperty(property);
        if (v != null) {
            return v;
        } else {
            v = System.getenv(variable);
            if (v != null) {
                return v;
            } else {
                if (value != null) {
                    return value;
                } else {
                    return def;
                }
            }
        }
    }
}

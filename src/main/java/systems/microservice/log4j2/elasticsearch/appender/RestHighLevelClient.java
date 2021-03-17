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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileConstants;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

/**
 * @author Dmitry Kotlyarov
 * @since 2.0
 */
final class RestHighLevelClient {
    private static final SmileFactory SMILE_FACTORY = new SmileFactory();

    private final URL[] urls;

    public RestHighLevelClient(URL[] urls) {
        this.urls = urls;
    }

    private boolean indexExists(URL url, String index) {
        try {
            URL u = new URL(url, index);
            HttpURLConnection conn = (HttpURLConnection) u.openConnection();
            conn.setRequestMethod("HEAD");
            conn.setConnectTimeout(30000);
            conn.setUseCaches(false);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestProperty("Content-Type", "application/smile");
            conn.setRequestProperty("Accept", "application/smile");
            conn.connect();
            try {
                return conn.getResponseCode() == HttpURLConnection.HTTP_OK;
            } finally {
                conn.disconnect();
            }
        } catch (IOException ex) {
            return false;
        }
    }

    private boolean createIndex(URL url, String index) {
        try {
            URL u = new URL(url, index);
            HttpURLConnection conn = (HttpURLConnection) u.openConnection();
            conn.setRequestMethod("PUT");
            conn.setConnectTimeout(30000);
            conn.setUseCaches(false);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestProperty("Content-Type", "application/smile");
            conn.setRequestProperty("Accept", "application/smile");
            conn.connect();
            try {
                try (OutputStream out = conn.getOutputStream()) {
                    try (SmileGenerator gen = SMILE_FACTORY.createGenerator(out)) {
                        gen.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
                        gen.writeStartObject();
                        {
                            gen.writeFieldName("mappings");
                            gen.writeStartObject();
                            {
                                gen.writeFieldName("properties");
                                gen.writeStartObject();
                                {
                                    gen.writeFieldName("time");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "date");
                                        gen.writeStringField("format", "epoch_millis");
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("type");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "keyword");
                                        gen.writeNumberField("ignore_above", 16);
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("platform");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "keyword");
                                        gen.writeNumberField("ignore_above", 16);
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("process");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("id");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("uuid");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 36);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("start");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "date");
                                                gen.writeStringField("format", "epoch_millis");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("finish");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "date");
                                                gen.writeStringField("format", "epoch_millis");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("variables");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "text");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("properties");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "text");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("cmdline");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "text");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("io");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "text");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("limits");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "text");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("mounts");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "text");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("net");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("dev");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "text");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                    gen.writeFieldName("protocols");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "text");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("host");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("name");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 256);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("ip");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 64);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("logger");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "keyword");
                                        gen.writeNumberField("ignore_above", 512);
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("thread");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("id");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("uuid");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 36);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("name");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 512);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("priority");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "integer");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("live");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "integer");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("daemon");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "integer");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("peak");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "integer");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("total");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("cpu");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("count");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "integer");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("m1");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "float");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("m5");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "float");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("m15");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "float");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("entity");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("active");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "integer");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                    gen.writeFieldName("total");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "integer");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("memory");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("heap");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("init");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "long");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                    gen.writeFieldName("used");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "long");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                    gen.writeFieldName("committed");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "long");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                    gen.writeFieldName("max");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "long");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("nonheap");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("init");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "long");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                    gen.writeFieldName("used");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "long");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                    gen.writeFieldName("committed");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "long");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                    gen.writeFieldName("max");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "long");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("object");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("pending");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeFieldName("properties");
                                                        gen.writeStartObject();
                                                        {
                                                            gen.writeFieldName("finalization");
                                                            gen.writeStartObject();
                                                            {
                                                                gen.writeStringField("type", "integer");
                                                                gen.writeBooleanField("index", true);
                                                            }
                                                            gen.writeEndObject();
                                                        }
                                                        gen.writeEndObject();
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("disk");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("total");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("free");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("usable");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("class");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("active");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "integer");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("loaded");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("unloaded");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("level");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "keyword");
                                        gen.writeNumberField("ignore_above", 256);
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("message");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "text");
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("appender");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("name");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 256);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("url");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "text");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("index");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 256);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("enable");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "boolean");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("count");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("max");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "integer");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("size");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("max");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "long");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("bulk");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("count");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeFieldName("properties");
                                                        gen.writeStartObject();
                                                        {
                                                            gen.writeFieldName("max");
                                                            gen.writeStartObject();
                                                            {
                                                                gen.writeStringField("type", "integer");
                                                                gen.writeBooleanField("index", true);
                                                            }
                                                            gen.writeEndObject();
                                                        }
                                                        gen.writeEndObject();
                                                    }
                                                    gen.writeEndObject();
                                                    gen.writeFieldName("size");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeFieldName("properties");
                                                        gen.writeStartObject();
                                                        {
                                                            gen.writeFieldName("max");
                                                            gen.writeStartObject();
                                                            {
                                                                gen.writeStringField("type", "long");
                                                                gen.writeBooleanField("index", true);
                                                            }
                                                            gen.writeEndObject();
                                                        }
                                                        gen.writeEndObject();
                                                    }
                                                    gen.writeEndObject();
                                                    gen.writeFieldName("retry");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeFieldName("properties");
                                                        gen.writeStartObject();
                                                        {
                                                            gen.writeFieldName("count");
                                                            gen.writeStartObject();
                                                            {
                                                                gen.writeStringField("type", "integer");
                                                                gen.writeBooleanField("index", true);
                                                            }
                                                            gen.writeEndObject();
                                                            gen.writeFieldName("delay");
                                                            gen.writeStartObject();
                                                            {
                                                                gen.writeStringField("type", "long");
                                                                gen.writeBooleanField("index", true);
                                                            }
                                                            gen.writeEndObject();
                                                        }
                                                        gen.writeEndObject();
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("delay");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("max");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "long");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("event");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("size");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeFieldName("properties");
                                                        gen.writeStartObject();
                                                        {
                                                            gen.writeFieldName("start");
                                                            gen.writeStartObject();
                                                            {
                                                                gen.writeFieldName("properties");
                                                                gen.writeStartObject();
                                                                {
                                                                    gen.writeFieldName("finish");
                                                                    gen.writeStartObject();
                                                                    {
                                                                        gen.writeStringField("type", "integer");
                                                                        gen.writeBooleanField("index", true);
                                                                    }
                                                                    gen.writeEndObject();
                                                                }
                                                                gen.writeEndObject();
                                                            }
                                                            gen.writeEndObject();
                                                            gen.writeFieldName("default");
                                                            gen.writeStartObject();
                                                            {
                                                                gen.writeStringField("type", "integer");
                                                                gen.writeBooleanField("index", true);
                                                            }
                                                            gen.writeEndObject();
                                                            gen.writeFieldName("exception");
                                                            gen.writeStartObject();
                                                            {
                                                                gen.writeStringField("type", "integer");
                                                                gen.writeBooleanField("index", true);
                                                            }
                                                            gen.writeEndObject();
                                                        }
                                                        gen.writeEndObject();
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("length");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("string");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeFieldName("properties");
                                                        gen.writeStartObject();
                                                        {
                                                            gen.writeFieldName("max");
                                                            gen.writeStartObject();
                                                            {
                                                                gen.writeStringField("type", "integer");
                                                                gen.writeBooleanField("index", true);
                                                            }
                                                            gen.writeEndObject();
                                                        }
                                                        gen.writeEndObject();
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("out");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "boolean");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("set");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("default");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeFieldName("properties");
                                                        gen.writeStartObject();
                                                        {
                                                            gen.writeFieldName("uncaught");
                                                            gen.writeStartObject();
                                                            {
                                                                gen.writeFieldName("properties");
                                                                gen.writeStartObject();
                                                                {
                                                                    gen.writeFieldName("exception");
                                                                    gen.writeStartObject();
                                                                    {
                                                                        gen.writeFieldName("properties");
                                                                        gen.writeStartObject();
                                                                        {
                                                                            gen.writeFieldName("handler");
                                                                            gen.writeStartObject();
                                                                            {
                                                                                gen.writeStringField("type", "boolean");
                                                                                gen.writeBooleanField("index", true);
                                                                            }
                                                                            gen.writeEndObject();
                                                                        }
                                                                        gen.writeEndObject();
                                                                    }
                                                                    gen.writeEndObject();
                                                                }
                                                                gen.writeEndObject();
                                                            }
                                                            gen.writeEndObject();
                                                        }
                                                        gen.writeEndObject();
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("source");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("file");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 512);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("class");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 512);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("method");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 512);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("line");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "integer");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("exception");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("class");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 512);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("message");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "text");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("stacktrace");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "text");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("suppressed");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("count");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "integer");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("cause");
                                            gen.writeStartObject();
                                            {
                                                gen.writeFieldName("properties");
                                                gen.writeStartObject();
                                                {
                                                    gen.writeFieldName("class");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "keyword");
                                                        gen.writeNumberField("ignore_above", 512);
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                    gen.writeFieldName("message");
                                                    gen.writeStartObject();
                                                    {
                                                        gen.writeStringField("type", "text");
                                                        gen.writeBooleanField("index", true);
                                                    }
                                                    gen.writeEndObject();
                                                }
                                                gen.writeEndObject();
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("marker");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("name");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "keyword");
                                                gen.writeNumberField("ignore_above", 512);
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("parents");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "boolean");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("size");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "integer");
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("total");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("count");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("size");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("lost");
                                    gen.writeStartObject();
                                    {
                                        gen.writeFieldName("properties");
                                        gen.writeStartObject();
                                        {
                                            gen.writeFieldName("count");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                            gen.writeFieldName("size");
                                            gen.writeStartObject();
                                            {
                                                gen.writeStringField("type", "long");
                                                gen.writeBooleanField("index", true);
                                            }
                                            gen.writeEndObject();
                                        }
                                        gen.writeEndObject();
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("environment");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "keyword");
                                        gen.writeNumberField("ignore_above", 512);
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("application");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "keyword");
                                        gen.writeNumberField("ignore_above", 512);
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("version");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "keyword");
                                        gen.writeNumberField("ignore_above", 512);
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("namespace");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "keyword");
                                        gen.writeNumberField("ignore_above", 512);
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("pod");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "keyword");
                                        gen.writeNumberField("ignore_above", 512);
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                    gen.writeFieldName("node");
                                    gen.writeStartObject();
                                    {
                                        gen.writeStringField("type", "keyword");
                                        gen.writeNumberField("ignore_above", 512);
                                        gen.writeBooleanField("index", true);
                                    }
                                    gen.writeEndObject();
                                }
                                gen.writeEndObject();
                            }
                            gen.writeEndObject();
                        }
                        gen.writeEndObject();
                        gen.flush();
                    }
                }
                return conn.getResponseCode() == HttpURLConnection.HTTP_OK;
            } finally {
                conn.disconnect();
            }
        } catch (IOException ex) {
            return false;
        }
    }

    private BulkResponse bulk(URL url, BulkRequest request) throws IOException {
        HashSet<String> idxs = new HashSet<>(32);
        List<InputLogEvent> es = request.events();
        for (InputLogEvent e : es) {
            String idx = e.index;
            if (!idxs.contains(idx)) {
                idxs.add(idx);
                if (!indexExists(url, idx)) {
                    createIndex(url, idx);
                }
            }
        }
        URL u = new URL(url, "_bulk");
        HttpURLConnection conn = (HttpURLConnection) u.openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(30000);
        conn.setUseCaches(false);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestProperty("Content-Type", "application/smile");
        conn.setRequestProperty("Accept", "application/smile");
        conn.connect();
        try {
            try (OutputStream out = conn.getOutputStream()) {
                for (InputLogEvent e : es) {
                    try (SmileGenerator gen = SMILE_FACTORY.createGenerator(out)) {
                        gen.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
                        gen.writeStartObject();
                        {
                            gen.writeFieldName("create");
                            gen.writeStartObject();
                            {
                                gen.writeStringField("_index", e.index);
                                gen.writeStringField("_id", e.id);
                            }
                            gen.writeEndObject();
                        }
                        gen.writeEndObject();
                        gen.flush();
                    }
                    out.write(SmileConstants.BYTE_MARKER_END_OF_CONTENT);
                    ByteArrayOutputStream d = e.data;
                    out.write(d.buffer(), 0, d.size());
                    out.write(SmileConstants.BYTE_MARKER_END_OF_CONTENT);
                }
            }
            try (InputStream in = conn.getInputStream()) {
                return SmileUtil.readBytes(in, BulkResponse.class);
            }
        } finally {
            conn.disconnect();
        }
    }

    public BulkResponse bulk(BulkRequest request) throws IOException {
        ArrayList<URL> us = new ArrayList<>(urls.length);
        Collections.addAll(us, urls);
        Random rnd = new Random();
        IOException e = null;
        while (!us.isEmpty()) {
            int i = rnd.nextInt(us.size());
            URL u = us.remove(i);
            try {
                return bulk(u, request);
            } catch (IOException ex) {
                e = ex;
            }
        }
        if (e == null) {
            return new BulkResponse();
        } else {
            throw e;
        }
    }
}

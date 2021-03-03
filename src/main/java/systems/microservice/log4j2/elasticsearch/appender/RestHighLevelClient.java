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
            conn.setRequestProperty("Accept", "application/json");
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
            conn.setRequestProperty("Accept", "application/json");
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
        conn.setRequestProperty("Accept", "application/json");
        conn.connect();
        try {
            try (OutputStream out = conn.getOutputStream()) {
                for (InputLogEvent e : es) {
                    try (SmileGenerator gen = SMILE_FACTORY.createGenerator(out)) {
                        gen.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
                        gen.writeStartObject(); {
                            gen.writeFieldName("create");
                            gen.writeStartObject(); {
                                gen.writeStringField("_index", e.index);
                                gen.writeStringField("_id", e.id);
                            } gen.writeEndObject();
                        } gen.writeEndObject();
                        gen.flush();
                    }
                    out.write(SmileConstants.BYTE_MARKER_END_OF_CONTENT);
                    ByteArrayOutputStream d = e.data;
                    out.write(d.buffer(), 0, d.size());
                    out.write(SmileConstants.BYTE_MARKER_END_OF_CONTENT);
                }
            }
            try (InputStream in = conn.getInputStream()) {
                return JsonUtil.readBytes(in, BulkResponse.class);
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
            BulkResponse r = new BulkResponse();
            r.took = 0L;
            r.errors = false;
            r.items = new LinkedList<>();
            return r;
        } else {
            throw e;
        }
    }
}

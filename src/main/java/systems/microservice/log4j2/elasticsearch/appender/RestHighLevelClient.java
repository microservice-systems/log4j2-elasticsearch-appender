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

    private BulkResponse bulk(URL url, BulkRequest request) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(30000);
        conn.setUseCaches(false);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestProperty("Content-Type", "application/smile");
        conn.setRequestProperty("Accept", "application/json");
        conn.connect();
        try {
            List<InputLogEvent> es = request.events();
            try (OutputStream out = conn.getOutputStream()) {
                try (SmileGenerator gen = SMILE_FACTORY.createGenerator(out)) {
                    for (InputLogEvent e : es) {
                        gen.writeStartObject(); {
                            gen.writeFieldName("create");
                            gen.writeStartObject(); {
                                gen.writeStringField("_index", e.index);
                                gen.writeStringField("_id", e.id);
                            } gen.writeEndObject();
                        } gen.writeEndObject();
                        gen.flush();
                        out.write(0xFF);
                        ByteArrayOutputStream d = e.data;
                        out.write(d.buffer(), 0, d.size());
                        out.write(0xFF);
                    }
                }
            }
            try (InputStream in = conn.getInputStream()) {
                return JsonUtil.readBytes(in, BulkResponse.class);
            }
        } finally {
            conn.disconnect();
        }
    }

    public BulkResponse bulk(BulkRequest request) throws Exception {
        ArrayList<URL> us = new ArrayList<>(urls.length);
        Collections.addAll(us, urls);
        Random rnd = new Random();
        Exception e = null;
        while (!us.isEmpty()) {
            int i = rnd.nextInt(us.size());
            URL u = us.remove(i);
            try {
                return bulk(u, request);
            } catch (Exception ex) {
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

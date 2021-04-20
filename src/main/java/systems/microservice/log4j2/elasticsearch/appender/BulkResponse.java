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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Dmitry Kotlyarov
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
final class BulkResponse {
    @JsonProperty("took")   public long took = 0L;
    @JsonProperty("errors") public boolean errors = false;
    @JsonProperty("items")  public List<Item> items = new LinkedList<>();

    public static final class Item {
        @JsonProperty("create") public Create create;

        public static final class Create {
            @JsonProperty("status")        public int status;
            @JsonProperty("result")        public String result;
            @JsonProperty("_index")        public String index;
            @JsonProperty("_type")         public String type;
            @JsonProperty("_id")           public String id;
            @JsonProperty("_version")      public String version;
            @JsonProperty("_seq_no")       public String seqNo;
            @JsonProperty("_primary_term") public String primaryTerm;
            @JsonProperty("_shards")       public Shards shards;
            @JsonProperty("error")         public Error error;

            public static final class Shards {
                @JsonProperty("total")      public int total;
                @JsonProperty("successful") public int successful;
                @JsonProperty("failed")     public int failed;
            }

            public static final class Error {
                @JsonProperty("type")       public String type;
                @JsonProperty("reason")     public String reason;
                @JsonProperty("index_uuid") public String indexUUID;
                @JsonProperty("shard")      public String shard;
                @JsonProperty("index")      public String index;
            }
        }
    }
}

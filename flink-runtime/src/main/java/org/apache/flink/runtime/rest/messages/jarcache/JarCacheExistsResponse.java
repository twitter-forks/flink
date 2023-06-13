/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.jarcache;

import org.apache.flink.runtime.rest.handler.jarcache.JarCacheExistsHandler;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Response for the {@link JarCacheExistsHandler}. */
public class JarCacheExistsResponse implements ResponseBody {
    public static final String JAR_CACHE_EXISTS_CACHED = "cached";

    @JsonProperty(JAR_CACHE_EXISTS_CACHED)
    public boolean exists;

    @JsonCreator
    public JarCacheExistsResponse(@JsonProperty(JAR_CACHE_EXISTS_CACHED) boolean exists) {
        this.exists = exists;
    }
}

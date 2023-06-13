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

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.jarcache.JarCacheExistsHandler;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/** Headers for the {@link JarCacheExistsHandler}. */
public class JarCacheExistsHeaders
        implements MessageHeaders<
                EmptyRequestBody, JarCacheExistsResponse, JarCacheExistsMessageParameters> {
    private static final JarCacheExistsHeaders INSTANCE = new JarCacheExistsHeaders();

    public static JarCacheExistsHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return "/jarcache/:" + JarIDPathParameter.KEY;
    }

    @Override
    public Class<JarCacheExistsResponse> getResponseClass() {
        return JarCacheExistsResponse.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "";
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public JarCacheExistsMessageParameters getUnresolvedMessageParameters() {
        return new JarCacheExistsMessageParameters();
    }

    @Override
    public Collection<RuntimeRestAPIVersion> getSupportedAPIVersions() {
        return Collections.singleton(RuntimeRestAPIVersion.V1);
    }

    private JarCacheExistsHeaders() {}
}

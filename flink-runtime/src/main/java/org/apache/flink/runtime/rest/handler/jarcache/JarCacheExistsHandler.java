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

package org.apache.flink.runtime.rest.handler.jarcache;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.jarcache.JarCacheExistsHeaders;
import org.apache.flink.runtime.rest.messages.jarcache.JarCacheExistsMessageParameters;
import org.apache.flink.runtime.rest.messages.jarcache.JarCacheExistsResponse;
import org.apache.flink.runtime.rest.messages.jarcache.JarID;
import org.apache.flink.runtime.rest.messages.jarcache.JarIDPathParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler to check if a jar exists in the jar cache. */
public class JarCacheExistsHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                JarCacheExistsResponse,
                JarCacheExistsMessageParameters> {
    private final Path cacheRoot;

    public static Path computeCachePath(Path root, JarID jarId) {
        final String hexId = jarId.toHexString();
        return new Path(new Path(root, hexId.substring(0, 2)), hexId + ".jar");
    }

    public JarCacheExistsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            Path cacheRoot) {
        super(leaderRetriever, timeout, responseHeaders, JarCacheExistsHeaders.getInstance());
        this.cacheRoot = cacheRoot;
    }

    @Override
    protected CompletableFuture<JarCacheExistsResponse> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        final JarID jarId = request.getPathParameter(JarIDPathParameter.class);
        final Path cacheLocation = computeCachePath(cacheRoot, jarId);

        try {
            final JarCacheExistsResponse response;
            FileSystem fs = cacheLocation.getFileSystem();
            if (fs.exists(cacheLocation)) {
                response = new JarCacheExistsResponse(true);
            } else {
                response = new JarCacheExistsResponse(false);
            }
            return CompletableFuture.completedFuture(response);
        } catch (Exception e) {
            throw new RestHandlerException(
                    "Error getting file information", HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}

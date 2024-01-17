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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.jarcache.JarCacheUploadHeaders;
import org.apache.flink.runtime.rest.messages.jarcache.JarCacheUploadMessageParameters;
import org.apache.flink.runtime.rest.messages.jarcache.JarID;
import org.apache.flink.runtime.rest.messages.jarcache.JarIDPathParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.IOUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashCode;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashingOutputStream;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.rest.handler.jarcache.JarCacheExistsHandler.computeCachePath;

/** Handler for jar cache uploads. */
public class JarCacheUploadHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                EmptyResponseBody,
                JarCacheUploadMessageParameters> {
    private final Path cacheRoot;

    public JarCacheUploadHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            Path cacheRoot) {
        super(leaderRetriever, timeout, responseHeaders, JarCacheUploadHeaders.getInstance());
        this.cacheRoot = cacheRoot;
    }

    private CompletableFuture<EmptyResponseBody> uploadFileToCache(
            File src, Path dest, byte[] expectedSha256) throws IOException {
        FileSystem fs = dest.getFileSystem();
        if (fs.exists(dest)) {
            return CompletableFuture.completedFuture(EmptyResponseBody.getInstance());
        }

        final Path tmp = dest.suffix("." + UUID.randomUUID() + "tmp");
        try (final InputStream in = Files.newInputStream(src.toPath());
                final FSDataOutputStream out = fs.create(tmp, FileSystem.WriteMode.NO_OVERWRITE);
                final HashingOutputStream shaStream =
                        new HashingOutputStream(Hashing.sha256(), out)) {

            IOUtils.copyBytes(in, shaStream, 65535, true);
            HashCode actualHash = shaStream.hash();
            HashCode expectedHash = HashCode.fromBytes(expectedSha256);

            final CompletableFuture<EmptyResponseBody> fut = new CompletableFuture<>();

            if (!actualHash.equals(expectedHash)) {
                fs.delete(tmp, false);
                fut.completeExceptionally(
                        new RestHandlerException(
                                "The file hash did not match the expected has",
                                HttpResponseStatus.BAD_REQUEST));
            } else {
                // TODO: handle this failing because another instance overwrote it
                fs.rename(tmp, dest);
                fut.complete(EmptyResponseBody.getInstance());
            }
            return fut;
        }
    }

    @Override
    protected CompletableFuture<EmptyResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        if (request.getUploadedFiles().size() != 1) {
            throw new IllegalArgumentException(
                    "Expected exactly one file, got " + request.getUploadedFiles().size());
        }

        final JarID jarId = request.getPathParameter(JarIDPathParameter.class);
        final Path cacheLocation = computeCachePath(cacheRoot, jarId);

        try {
            return uploadFileToCache(
                    Iterables.getOnlyElement(request.getUploadedFiles()),
                    cacheLocation,
                    jarId.getId());
        } catch (Exception e) {
            throw new RestHandlerException(
                    "Error getting file information", HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
}

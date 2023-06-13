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

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Contains utility methods for clients. */
public enum ClientUtils {
    ;

    private static final int NUM_PARALLEL_BLOB_UPLOADS = 16;

    private static final ExecutorService blobUploadService =
            Executors.newFixedThreadPool(
                    NUM_PARALLEL_BLOB_UPLOADS, new ExecutorThreadFactory("Flink-BlobClient-IO"));

    /**
     * Extracts all files required for the execution from the given {@link JobGraph} and uploads
     * them using the {@link BlobClient} from the given {@link Supplier}.
     *
     * @param jobGraph jobgraph requiring files
     * @param clientSupplier supplier of blob client to upload files with
     * @throws FlinkException if the upload fails
     */
    public static void extractAndUploadJobGraphFiles(
            JobGraph jobGraph, SupplierWithException<BlobClient, IOException> clientSupplier)
            throws FlinkException {
        List<Path> userJars = jobGraph.getUserJars();
        Collection<Tuple2<String, Path>> userArtifacts =
                jobGraph.getUserArtifacts().entrySet().stream()
                        .map(
                                entry ->
                                        Tuple2.of(
                                                entry.getKey(),
                                                new Path(entry.getValue().filePath)))
                        .collect(Collectors.toList());

        uploadJobGraphFiles(jobGraph, userJars, userArtifacts, clientSupplier);
    }

    /**
     * Uploads the given jars and artifacts required for the execution of the given {@link JobGraph}
     * using the {@link BlobClient} from the given {@link Supplier}.
     *
     * @param jobGraph jobgraph requiring files
     * @param userJars jars to upload
     * @param userArtifacts artifacts to upload
     * @param clientSupplier supplier of blob client to upload files with
     * @throws FlinkException if the upload fails
     */
    public static void uploadJobGraphFiles(
            JobGraph jobGraph,
            Collection<Path> userJars,
            Collection<Tuple2<String, org.apache.flink.core.fs.Path>> userArtifacts,
            SupplierWithException<BlobClient, IOException> clientSupplier)
            throws FlinkException {
        if (!userJars.isEmpty() || !userArtifacts.isEmpty()) {
            try (BlobClient client = clientSupplier.get()) {
                uploadAndSetUserJars(jobGraph, userJars, clientSupplier);
                uploadAndSetUserArtifacts(jobGraph, userArtifacts, client);
            } catch (IOException ioe) {
                throw new FlinkException("Could not upload job files.", ioe);
            }
        }
        jobGraph.writeUserArtifactEntriesToConfiguration();
    }

    /**
     * Uploads the given user jars using the given {@link BlobClient}, and sets the appropriate
     * blobkeys on the given {@link JobGraph}.
     *
     * @param jobGraph jobgraph requiring user jars
     * @param userJars jars to upload
     * @param blobClient client to upload jars with
     * @throws IOException if the upload fails
     */
    private static void uploadAndSetUserJars(
            JobGraph jobGraph,
            Collection<Path> userJars,
            SupplierWithException<BlobClient, IOException> blobClient)
            throws IOException {
        Collection<PermanentBlobKey> blobKeys =
                uploadUserJars(jobGraph.getJobID(), userJars, blobClient);
        setUserJarBlobKeys(blobKeys, jobGraph);
    }

    private static Collection<PermanentBlobKey> uploadUserJars(
            JobID jobId,
            Collection<Path> userJars,
            SupplierWithException<BlobClient, IOException> blobClientSupplier)
            throws IOException {

        final List<Future<?>> uploadFutures = new ArrayList<>(NUM_PARALLEL_BLOB_UPLOADS);
        final List<CompletableFuture<PermanentBlobKey>> uploadedBlobKeyFutures =
                new ArrayList<>(userJars.size());
        final ConcurrentLinkedQueue<Tuple2<Path, CompletableFuture<PermanentBlobKey>>> toUpload =
                new ConcurrentLinkedQueue<>();

        for (Path jar : userJars) {
            final CompletableFuture<PermanentBlobKey> fut = new CompletableFuture<>();
            uploadedBlobKeyFutures.add(fut);
            toUpload.add(Tuple2.of(jar, fut));
        }

        for (int id = 0; id < NUM_PARALLEL_BLOB_UPLOADS; id++) {
            uploadFutures.add(
                    blobUploadService.submit(
                            () -> {
                                try (BlobClient blobClient = blobClientSupplier.get()) {
                                    Tuple2<Path, CompletableFuture<PermanentBlobKey>> next;
                                    while ((next = toUpload.poll()) != null) {
                                        Path jar = next.f0;
                                        CompletableFuture<PermanentBlobKey> fut = next.f1;
                                        try {
                                            fut.complete(blobClient.uploadFile(jobId, jar));
                                        } catch (IOException ex) {
                                            fut.completeExceptionally(ex);
                                        }
                                    }
                                }
                                return null;
                            }));
        }

        for (Future<?> uploadFuture : uploadFutures) {
            try {
                uploadFuture.get();
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }

        final List<PermanentBlobKey> uploadedBlobKeys =
                new ArrayList<>(uploadedBlobKeyFutures.size());
        for (CompletableFuture<PermanentBlobKey> fut : uploadedBlobKeyFutures) {
            try {
                uploadedBlobKeys.add(fut.get());
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
        return uploadedBlobKeys;
    }

    private static void setUserJarBlobKeys(
            Collection<PermanentBlobKey> blobKeys, JobGraph jobGraph) {
        blobKeys.forEach(jobGraph::addUserJarBlobKey);
    }

    /**
     * Uploads the given user artifacts using the given {@link BlobClient}, and sets the appropriate
     * blobkeys on the given {@link JobGraph}.
     *
     * @param jobGraph jobgraph requiring user artifacts
     * @param artifactPaths artifacts to upload
     * @param blobClient client to upload artifacts with
     * @throws IOException if the upload fails
     */
    private static void uploadAndSetUserArtifacts(
            JobGraph jobGraph,
            Collection<Tuple2<String, Path>> artifactPaths,
            BlobClient blobClient)
            throws IOException {
        Collection<Tuple2<String, PermanentBlobKey>> blobKeys =
                uploadUserArtifacts(jobGraph.getJobID(), artifactPaths, blobClient);
        setUserArtifactBlobKeys(jobGraph, blobKeys);
    }

    private static Collection<Tuple2<String, PermanentBlobKey>> uploadUserArtifacts(
            JobID jobID, Collection<Tuple2<String, Path>> userArtifacts, BlobClient blobClient)
            throws IOException {
        Collection<Tuple2<String, PermanentBlobKey>> blobKeys =
                new ArrayList<>(userArtifacts.size());
        for (Tuple2<String, Path> userArtifact : userArtifacts) {
            // only upload local files
            if (!userArtifact.f1.getFileSystem().isDistributedFS()) {
                final PermanentBlobKey blobKey = blobClient.uploadFile(jobID, userArtifact.f1);
                blobKeys.add(Tuple2.of(userArtifact.f0, blobKey));
            }
        }
        return blobKeys;
    }

    private static void setUserArtifactBlobKeys(
            JobGraph jobGraph, Collection<Tuple2<String, PermanentBlobKey>> blobKeys)
            throws IOException {
        for (Tuple2<String, PermanentBlobKey> blobKey : blobKeys) {
            jobGraph.setUserArtifactBlobKey(blobKey.f0, blobKey.f1);
        }
    }
}

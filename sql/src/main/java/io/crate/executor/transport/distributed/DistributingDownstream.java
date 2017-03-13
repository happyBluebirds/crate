/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport.distributed;

import com.google.common.annotations.VisibleForTesting;
import io.crate.Streamer;
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.operation.projectors.*;
import org.elasticsearch.common.logging.ESLogger;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * RowReceiver that sends rows as paged requests to other hosts.
 *
 * How the data is "bucketed" depends on the {@code MultiBucketBuilder} implementation.
 *
 * (Simplified) Workflow:
 *
 * <pre>
 *      [upstream-loop]:
 *
 *          +--> upstream:
 *          |     |
 *          |     | emits rows
 *          |     |
 *          |     v
 *          |   setNextRow
 *          |     |
 *          |     | add to BucketBuilder
 *          |     v
 *          |     pageSizeReached?
 *          |         /    \
 *          |        /      \    [request-loop]:
 *          +--------        \
 *    [continue|pause]        \
 *                           trySendRequests
 *                          /   |
 *                         /    |
 *                  +------     |
 *                  |           v
 *                  |       Downstream
 *                  |           |
 *                  |  response v
 *                  +<----------+
 * </pre>
 *
 * Note that the upstream-loop will *not* suspend if the pageSize is reached
 * but there are no "in-flight" requests.
 *
 * Both, the upstream-loop and request-loop can be running at the same time.
 * {@code trySendRequests} has some synchronization to make sure that there are
 * never two pages send concurrently.
 *
 */
public class DistributingDownstream implements RowReceiver {

    private final ESLogger logger;
    private final UUID jobId;
    private final int targetPhaseId;
    private final byte inputId;
    private final int bucketIdx;
    private final TransportDistributedResultAction distributedResultAction;
    private final int pageSize;
    private final Streamer<?>[] streamers;
    private final Collection<String> downstreamNodeIds;
    private final CompletableFuture<Void> finishFuture = new CompletableFuture<>();

    @VisibleForTesting
    final MultiBucketBuilder multiBucketBuilder;


    public DistributingDownstream(ESLogger logger,
                                  UUID jobId,
                                  MultiBucketBuilder multiBucketBuilder,
                                  int targetPhaseId,
                                  byte inputId,
                                  int bucketIdx,
                                  Collection<String> downstreamNodeIds,
                                  TransportDistributedResultAction distributedResultAction,
                                  Streamer<?>[] streamers,
                                  int pageSize) {
        this.logger = logger;
        this.multiBucketBuilder = multiBucketBuilder;
        this.jobId = jobId;
        this.targetPhaseId = targetPhaseId;
        this.inputId = inputId;
        this.bucketIdx = bucketIdx;
        this.distributedResultAction = distributedResultAction;
        this.pageSize = pageSize;
        this.streamers = streamers;
        this.downstreamNodeIds = downstreamNodeIds;
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return finishFuture;
    }

    @Override
    public Result setNextRow(Row row) {
        throw new UnsupportedOperationException("DistributingDownstream must be used as BatchConsumer");
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeable) {

    }

    @Override
    public void finish(RepeatHandle repeatable) {
        throw new UnsupportedOperationException("DistributingDownstream must be used as BatchConsumer");
    }

    @Override
    public void fail(Throwable throwable) {
        throw new UnsupportedOperationException("DistributingDownstream must be used as BatchConsumer");
    }

    @Override
    public void kill(Throwable throwable) {

    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    @Nullable
    @Override
    public BatchConsumer asConsumer() {
        return new DistributingConsumer(
            logger,
            jobId,
            multiBucketBuilder,
            targetPhaseId,
            inputId,
            bucketIdx,
            downstreamNodeIds,
            distributedResultAction,
            streamers,
            pageSize,
            finishFuture
        );
    }
}

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

import io.crate.Streamer;
import io.crate.data.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributingConsumer implements BatchConsumer {

    private final ESLogger logger;
    private final UUID jobId;
    private final MultiBucketBuilder multiBucketBuilder;
    private final int targetPhaseId;
    private final byte inputId;
    private final int bucketIdx;
    private final TransportDistributedResultAction distributedResultAction;
    private final Streamer<?>[] streamers;
    private final int pageSize;
    private final Bucket[] buckets;
    private final List<Downstream> downstreams;
    private final CompletableFuture<Void> finishFuture;
    private final boolean traceEnabled;

    private volatile Throwable failure;

    public DistributingConsumer(ESLogger logger,
                                UUID jobId,
                                MultiBucketBuilder multiBucketBuilder,
                                int targetPhaseId,
                                byte inputId,
                                int bucketIdx,
                                Collection<String> downstreamNodeIds,
                                TransportDistributedResultAction distributedResultAction,
                                Streamer<?>[] streamers,
                                int pageSize,
                                CompletableFuture<Void> finishFuture) {
        this.traceEnabled = logger.isTraceEnabled();
        this.logger = logger;
        this.jobId = jobId;
        this.multiBucketBuilder = multiBucketBuilder;
        this.targetPhaseId = targetPhaseId;
        this.inputId = inputId;
        this.bucketIdx = bucketIdx;
        this.distributedResultAction = distributedResultAction;
        this.streamers = streamers;
        this.pageSize = pageSize;
        this.buckets = new Bucket[downstreamNodeIds.size()];
        downstreams = new ArrayList<>(downstreamNodeIds.size());
        this.finishFuture = finishFuture;
        for (String downstreamNodeId : downstreamNodeIds) {
            downstreams.add(new Downstream(downstreamNodeId));
        }
    }

    @Override
    public void accept(BatchIterator iterator, @Nullable Throwable failure) {
        if (failure == null) {
            consumeIt(iterator);
        } else {
            forwardFailure(null, failure);
        }
    }

    private void consumeIt(BatchIterator it) {
        Row row = RowBridging.toRow(it.rowData());
        try {
            while (it.moveNext()) {
                multiBucketBuilder.add(row);
                if (multiBucketBuilder.size() >= pageSize) {
                    forwardResults(it, false);
                    return;
                }
            }
        } catch (Throwable t) {
            forwardFailure(it, t);
            return;
        }
        if (it.allLoaded()) {
            forwardResults(it, true);
        } else {
            it.loadNextBatch().whenComplete((r, t) -> {
                if (t == null) {
                    consumeIt(it);
                } else {
                    forwardFailure(it, t);
                }
            });
        }
    }

    private void forwardFailure(@Nullable final BatchIterator it, final Throwable failure) {
        AtomicInteger numActiveRequests = new AtomicInteger(downstreams.size());
        DistributedResultRequest request =
            new DistributedResultRequest(jobId, targetPhaseId, inputId, bucketIdx, failure, false);
        for (int i = 0; i < downstreams.size(); i++) {
            Downstream downstream = downstreams.get(i);
            if (downstream.needsMoreData == false) {
                countdownAndMaybeCloseIt(numActiveRequests, failure, it);
            } else {
                if (traceEnabled) {
                    logger.trace("forwardFailure targetNode={} targetPhase={}/{} bucket={} failure={}",
                        downstream.nodeId, targetPhaseId, inputId, bucketIdx, failure);
                }
                distributedResultAction.pushResult(downstream.nodeId, request, new ActionListener<DistributedResultResponse>() {
                    @Override
                    public void onResponse(DistributedResultResponse response) {
                        downstream.needsMoreData = false;
                        countdownAndMaybeCloseIt(numActiveRequests, failure, it);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.error("Error sending failure to downstream={} targetPhase={}/{} bucket={}", e,
                            downstream.nodeId, targetPhaseId, inputId, bucketIdx);
                        countdownAndMaybeCloseIt(numActiveRequests, failure, it);
                    }
                });
            }
        }
    }

    private void countdownAndMaybeCloseIt(AtomicInteger numActiveRequests, Throwable failure, @Nullable BatchIterator it) {
        if (numActiveRequests.decrementAndGet() == 0) {
            if (it != null) {
                it.close();
            }
            finishFuture.completeExceptionally(failure);
        }
    }

    private void forwardResults(BatchIterator it, boolean isLast) {
        multiBucketBuilder.build(buckets);

        AtomicInteger numActiveRequests = new AtomicInteger(downstreams.size());
        for (int i = 0; i < downstreams.size(); i++) {
            Downstream downstream = downstreams.get(i);
            if (downstream.needsMoreData == false) {
                countdownAndMaybeContinue(it, numActiveRequests);
                continue;
            }
            if (traceEnabled) {
                logger.trace("forwardResults targetNode={} targetPhase={}/{} bucket={} isLast={}",
                    downstream.nodeId, targetPhaseId, inputId, bucketIdx, isLast);
            }
            distributedResultAction.pushResult(
                downstream.nodeId,
                new DistributedResultRequest(jobId, targetPhaseId, inputId, bucketIdx, streamers, buckets[i], isLast),
                new ActionListener<DistributedResultResponse>() {
                    @Override
                    public void onResponse(DistributedResultResponse response) {
                        downstream.needsMoreData = response.needMore();
                        countdownAndMaybeContinue(it, numActiveRequests);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        failure = e;
                        downstream.needsMoreData = false;
                        countdownAndMaybeContinue(it, numActiveRequests);
                    }
                }
            );
        }
    }

    private void countdownAndMaybeContinue(BatchIterator it, AtomicInteger numActiveRequests) {
        if (numActiveRequests.decrementAndGet() == 0) {
            if (downstreams.stream().anyMatch(Downstream::needsMoreData)) {
                consumeIt(it);
            } else {
                it.close();
                Throwable failure = this.failure;
                if (failure == null) {
                    finishFuture.complete(null);
                } else {
                    finishFuture.completeExceptionally(failure);
                }
            }
        }
    }

    private static class Downstream {

        private final String nodeId;
        private boolean needsMoreData = true;

        Downstream(String nodeId) {
            this.nodeId = nodeId;
        }

        boolean needsMoreData() {
            return needsMoreData;
        }
    }
}

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

package io.crate.data;

import io.crate.exceptions.Exceptions;

import javax.annotation.Nullable;
import java.util.concurrent.CompletionStage;

public class KillableBatchIterator extends ForwardingBatchIterator implements Killable {

    private final BatchIterator delegate;
    private volatile Throwable failure;
    private CompletionStage<?> loading;

    public KillableBatchIterator(BatchIterator delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean moveNext() {
        if (failure == null) {
            return super.moveNext();
        }
        Exceptions.rethrowUnchecked(failure);
        return false;
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        loading = super.loadNextBatch().whenComplete((r, f) -> loading = null);
        return loading;
    }

    @Override
    protected BatchIterator delegate() {
        return delegate;
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        if (throwable == null) {
            throwable = new InterruptedException("Job killed");
        }
        failure = throwable;

        // FIXME: this doesn't propagate
        if (loading != null) {
            loading.toCompletableFuture().cancel(true);
        }
        close();
    }
}

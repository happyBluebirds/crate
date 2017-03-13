/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.projectors;

import io.crate.data.*;
import io.crate.operation.collect.CollectExpression;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

final class SortingProjector {

    public static BatchIterator create(BatchIterator batchIterator,
                                       Collection<? extends Input<?>> inputs,
                                       Iterable<? extends CollectExpression<Row, ?>> expressions,
                                       int numOutputs,
                                       Comparator<Object[]> comparator,
                                       int offset) {

        Collector<Row, ?, Bucket> collector = Collectors.mapping(
            row -> getCells(row, inputs, expressions),
            Collectors.collectingAndThen(
                Collectors.toList(),
                rows -> sortAndCreateBucket(rows, comparator, numOutputs, offset))
        );
        return CollectingBatchIterator.newInstance(batchIterator, collector, numOutputs);
    }

    private static Object[] getCells(Row row,
                              Collection<? extends Input<?>> inputs,
                              Iterable<? extends CollectExpression<Row, ?>> expressions) {
        for (CollectExpression<Row, ?> collectExpression : expressions) {
            collectExpression.setNextRow(row);
        }
        Object[] newRow = new Object[inputs.size()];
        int i = 0;
        for (Input<?> input : inputs) {
            newRow[i++] = input.value();
        }
        return newRow;
    }

    private static Bucket sortAndCreateBucket(List<Object[]> rows, Comparator<Object[]> comparator, int numOutputs, int offset) {
        rows.sort(comparator.reversed());
        if (offset == 0) {
            return new CollectionBucket(rows, numOutputs);
        }
        return new CollectionBucket(rows.subList(offset, rows.size()), numOutputs);
    }
}

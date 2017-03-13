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

import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.operation.AggregationContext;
import io.crate.operation.aggregation.Aggregator;
import io.crate.operation.collect.CollectExpression;

import java.util.Collections;
import java.util.stream.Collectors;

public class AggregationPipe {

    public static BatchIterator create (BatchIterator iterator,
                                        Iterable<CollectExpression<Row, ?>> expressions,
                                        AggregationContext[] aggregations,
                                        RamAccountingContext ramAccountingContext) {
        Aggregator[] aggregators = new Aggregator[aggregations.length];
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = new Aggregator(
                ramAccountingContext,
                aggregations[i].symbol(),
                aggregations[i].function(),
                aggregations[i].inputs()
            );
        }
        return CollectingBatchIterator.newInstance(
            iterator,
            Collectors.collectingAndThen(
                new AggregateCollector(expressions, aggregators),
                cells -> Collections.singletonList(new RowN(cells))
            ),
            aggregators.length
        );
    }
}

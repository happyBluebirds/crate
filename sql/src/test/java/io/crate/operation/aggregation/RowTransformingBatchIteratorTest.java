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

package io.crate.operation.aggregation;

import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.data.Row;
import io.crate.data.RowsBatchIterator;
import io.crate.operation.Input;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.scalar.arithmetic.AddFunction;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.BatchIteratorTester;
import io.crate.testing.RowGenerator;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.crate.testing.TestingHelpers.getFunctions;

public class RowTransformingBatchIteratorTest extends CrateUnitTest {

    private Iterable<Row> rows = RowGenerator.range(0, 10);
    private List<Input<?>> inputs;
    private Collection<CollectExpression<Row, ?>> expressions;

    private List<Object[]> expectedResult = StreamSupport.stream(rows.spliterator(), false)
        .map(r -> new Object[] { (long) r.get(0) + 2L })
        .collect(Collectors.toList());

    @Before
    public void createInputs() throws Exception {
        InputFactory inputFactory = new InputFactory(getFunctions());
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns();
        inputs = Collections.singletonList(ctx.add(AddFunction.of(new InputColumn(0), Literal.of(2L))));
        expressions = ctx.expressions();
    }

    @Test
    public void testRowTransformingIterator() throws Exception {
        BatchIteratorTester tester = new BatchIteratorTester(
            () -> new RowTransformingBatchIterator(RowsBatchIterator.newInstance(rows),
                inputs,
                expressions),
            expectedResult
        );
        tester.run();
    }
}
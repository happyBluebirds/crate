/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.lucene;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.collect.Sets;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.operation.operator.*;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Answers;

import java.util.Arrays;

import static io.crate.testing.TestingHelpers.createFunction;
import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class LuceneQueryBuilderTest extends RandomizedTest{

    private LuceneQueryBuilder builder;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        Functions functions = new ModulesBuilder()
                .add(new OperatorModule()).createInjector().getInstance(Functions.class);
        builder = new LuceneQueryBuilder(functions,
                mock(SearchContext.class, Answers.RETURNS_MOCKS.get()),
                mock(IndexCache.class, Answers.RETURNS_MOCKS.get()));
    }

    @Test
    public void testWhereRefEqRef() throws Exception {
        Reference foo = createReference("foo", DataTypes.STRING);
        Query query = convert(eq(foo, foo));
        assertThat(query, instanceOf(FilteredQuery.class));
    }

    @Test
    public void testLteQuery() throws Exception {
        Query query = convert(new WhereClause(createFunction(LteOperator.NAME,
                DataTypes.BOOLEAN,
                createReference("x", DataTypes.INTEGER),
                Literal.newLiteral(10))));
        assertThat(query, instanceOf(NumericRangeQuery.class));
        assertThat(query.toString(), is("x:{* TO 10]"));
    }

    @Test
    public void testEqOnTwoArraysBecomesGenericFunctionQuery() throws Exception {
        expectedException.expect(UnsupportedFeatureException.class);
        expectedException.expectMessage("Cannot compare two arrays");
        DataType longArray = new ArrayType(DataTypes.LONG);
        convert(new WhereClause(createFunction(EqOperator.NAME,
                DataTypes.BOOLEAN,
                createReference("x", longArray),
                Literal.newLiteral(longArray, new Object[] { 10L, 20L }))));
    }

    @Test
    public void testGteQuery() throws Exception {
        Query query = convert(new WhereClause(createFunction(GteOperator.NAME,
                DataTypes.BOOLEAN,
                createReference("x", DataTypes.INTEGER),
                Literal.newLiteral(10))));
        assertThat(query, instanceOf(NumericRangeQuery.class));
        assertThat(query.toString(), is("x:[10 TO *}"));
    }


    @Test
    public void testWhereRefInSetLiteralIsConvertedToBooleanQuery() throws Exception {
        DataType dataType = new SetType(DataTypes.STRING);
        Reference foo = createReference("foo", DataTypes.STRING);
        WhereClause whereClause = new WhereClause(
                createFunction(InOperator.NAME, DataTypes.BOOLEAN,
                        foo,
                        Literal.newLiteral(dataType, Sets.newHashSet(new BytesRef("foo"), new BytesRef("bar")))
                ));
        Query query = convert(whereClause);
        assertThat(query, instanceOf(BooleanQuery.class));
        for (BooleanClause booleanClause : (BooleanQuery) query) {
            assertThat(booleanClause.getQuery(), instanceOf(TermQuery.class));
        }
    }


    private Query convert(WhereClause eq) {
        return builder.convert(eq).query;
    }

    private WhereClause eq(Symbol left, Symbol right) {
        return new WhereClause(new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.asList(left.valueType(), right.valueType())), DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(left, right)
        ));
    }
}
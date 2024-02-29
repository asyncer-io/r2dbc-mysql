/*
 * Copyright 2024 asyncer.io projects
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.asyncer.r2dbc.mysql;

import io.asyncer.r2dbc.mysql.api.MySqlTransactionDefinition;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.TransactionDefinition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link StartTransactionState}.
 */
class StartTransactionStateTest {

    @ParameterizedTest
    @MethodSource
    void buildStartTransaction(TransactionDefinition definition, String excepted) {
        assertThat(StartTransactionState.buildStartTransaction(definition)).isEqualTo(excepted);
    }

    static Stream<Arguments> buildStartTransaction() {
        return Stream.of(
            Arguments.of(MySqlTransactionDefinition.empty(), "BEGIN"),
            Arguments.of(MySqlTransactionDefinition.from(IsolationLevel.READ_UNCOMMITTED), "BEGIN"),
            Arguments.of(MySqlTransactionDefinition.mutability(false), "START TRANSACTION READ ONLY"),
            Arguments.of(MySqlTransactionDefinition.mutability(true), "START TRANSACTION READ WRITE"),
            Arguments.of(
                MySqlTransactionDefinition.empty().consistent(),
                "START TRANSACTION WITH CONSISTENT SNAPSHOT"
            ),
            Arguments.of(
                MySqlTransactionDefinition.mutability(false).consistent(),
                "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"
            ),
            Arguments.of(
                MySqlTransactionDefinition.mutability(true).consistent(),
                "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ WRITE"
            ),
            Arguments.of(
                MySqlTransactionDefinition.mutability(false)
                    .consistent("ROCKSDB", 3L),
                "START TRANSACTION WITH CONSISTENT ROCKSDB SNAPSHOT FROM SESSION 3, READ ONLY"
            )
        );
    }
}

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

package io.asyncer.r2dbc.mysql.api;

import io.asyncer.r2dbc.mysql.ConsistentSnapshotEngine;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.TransactionDefinition;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MySqlTransactionDefinition}.
 */
class MySqlTransactionDefinitionTest {

    @Test
    void getAttribute() {
        Duration lockWaitTimeout = Duration.ofSeconds(118);
        long sessionId = 123456789L;
        MySqlTransactionDefinition definition = MySqlTransactionDefinition.from(IsolationLevel.READ_COMMITTED)
            .lockWaitTimeout(lockWaitTimeout)
            .consistent("ROCKSDB", sessionId);

        assertThat(definition.getAttribute(TransactionDefinition.ISOLATION_LEVEL))
            .isSameAs(IsolationLevel.READ_COMMITTED);
        assertThat(definition.getAttribute(TransactionDefinition.LOCK_WAIT_TIMEOUT))
            .isSameAs(lockWaitTimeout);
        assertThat(definition.getAttribute(MySqlTransactionDefinition.WITH_CONSISTENT_SNAPSHOT))
            .isTrue();
        assertThat(definition.getAttribute(MySqlTransactionDefinition.CONSISTENT_SNAPSHOT_ENGINE))
            .isEqualTo("ROCKSDB");
        assertThat(definition.getAttribute(MySqlTransactionDefinition.CONSISTENT_SNAPSHOT_FROM_SESSION))
            .isEqualTo(sessionId);
    }

    @Test
    void isolationLevel() {
        Duration lockWaitTimeout = Duration.ofSeconds(118);
        MySqlTransactionDefinition def1 = MySqlTransactionDefinition.mutability(false)
            .isolationLevel(IsolationLevel.SERIALIZABLE)
            .lockWaitTimeout(lockWaitTimeout);
        MySqlTransactionDefinition def2 = def1.isolationLevel(IsolationLevel.READ_COMMITTED);

        assertThat(def1).isNotEqualTo(def2);
        assertThat(def1.getAttribute(TransactionDefinition.LOCK_WAIT_TIMEOUT))
            .isSameAs(def2.getAttribute(TransactionDefinition.LOCK_WAIT_TIMEOUT))
            .isSameAs(lockWaitTimeout);
        assertThat(def1.getAttribute(TransactionDefinition.ISOLATION_LEVEL))
            .isSameAs(IsolationLevel.SERIALIZABLE);
        assertThat(def2.getAttribute(TransactionDefinition.ISOLATION_LEVEL))
            .isSameAs(IsolationLevel.READ_COMMITTED);
        assertThat(def1.getAttribute(TransactionDefinition.READ_ONLY))
            .isSameAs(def2.getAttribute(TransactionDefinition.READ_ONLY))
            .isEqualTo(true);
    }

    @ParameterizedTest
    @MethodSource
    void withoutIsolationLevel(MySqlTransactionDefinition definition, IsolationLevel level) {
        assertThat(definition.getAttribute(TransactionDefinition.ISOLATION_LEVEL))
            .isSameAs(level);
        assertThat(definition.withoutIsolationLevel().getAttribute(TransactionDefinition.ISOLATION_LEVEL))
            .isNull();
    }

    @ParameterizedTest
    @MethodSource
    void withoutLockWaitTimeout(MySqlTransactionDefinition definition, @Nullable Duration lockWaitTimeout) {
        assertThat(definition.getAttribute(TransactionDefinition.LOCK_WAIT_TIMEOUT))
            .isEqualTo(lockWaitTimeout);
        assertThat(definition.withoutLockWaitTimeout().getAttribute(TransactionDefinition.LOCK_WAIT_TIMEOUT))
            .isNull();
    }

    @ParameterizedTest
    @MethodSource
    void withoutMutability(MySqlTransactionDefinition definition, @Nullable Boolean readOnly) {
        assertThat(definition.getAttribute(TransactionDefinition.READ_ONLY))
            .isEqualTo(readOnly);
        assertThat(definition.withoutMutability().getAttribute(TransactionDefinition.READ_ONLY))
            .isNull();
    }

    @ParameterizedTest
    @MethodSource
    void withoutConsistent(MySqlTransactionDefinition definition) {
        assertThat(definition.getAttribute(MySqlTransactionDefinition.WITH_CONSISTENT_SNAPSHOT))
            .isTrue();
        assertThat(definition)
            .isNotEqualTo(MySqlTransactionDefinition.empty())
            .extracting(MySqlTransactionDefinition::withoutConsistent)
            .isEqualTo(MySqlTransactionDefinition.empty());
    }

    static Stream<Arguments> withoutIsolationLevel() {
        return Stream.of(
            Arguments.of(MySqlTransactionDefinition.empty(), null),
            Arguments.of(
                MySqlTransactionDefinition.from(IsolationLevel.READ_COMMITTED),
                IsolationLevel.READ_COMMITTED
            ),
            Arguments.of(
                MySqlTransactionDefinition.from(IsolationLevel.SERIALIZABLE)
                    .lockWaitTimeout(Duration.ofSeconds(118)),
                IsolationLevel.SERIALIZABLE
            )
        );
    }

    static Stream<Arguments> withoutLockWaitTimeout() {
        return Stream.of(
            Arguments.of(MySqlTransactionDefinition.empty(), null),
            Arguments.of(
                MySqlTransactionDefinition.empty()
                    .lockWaitTimeout(Duration.ofSeconds(118)),
                Duration.ofSeconds(118)
            ),
            Arguments.of(
                MySqlTransactionDefinition.empty()
                    .lockWaitTimeout(Duration.ofSeconds(123))
                    .consistent("ROCKSDB", 123456789),
                Duration.ofSeconds(123)
            )
        );
    }

    static Stream<Arguments> withoutMutability() {
        return Stream.of(
            Arguments.of(MySqlTransactionDefinition.empty(), null),
            Arguments.of(MySqlTransactionDefinition.mutability(true), false),
            Arguments.of(MySqlTransactionDefinition.mutability(false), true),
            Arguments.of(MySqlTransactionDefinition.mutability(true).consistent(), false),
            Arguments.of(MySqlTransactionDefinition.mutability(false).consistent(), true),
            Arguments.of(
                MySqlTransactionDefinition.mutability(true)
                    .isolationLevel(IsolationLevel.SERIALIZABLE),
                false
            )
        );
    }

    static Stream<MySqlTransactionDefinition> withoutConsistent() {
        return Stream.of(
            MySqlTransactionDefinition.empty().consistent(),
            MySqlTransactionDefinition.empty().consistent("ROCKSDB"),
            MySqlTransactionDefinition.empty().consistent("ROCKSDB", 123456789),
            MySqlTransactionDefinition.empty().consistentFromSession(123456789)
        );
    }
}

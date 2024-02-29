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

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;

import java.time.Duration;

/**
 * {@link TransactionDefinition} for a MySQL database.
 *
 * @since 1.1.3
 */
public interface MySqlTransactionDefinition extends TransactionDefinition {

    /**
     * Use {@code WITH CONSISTENT SNAPSHOT} property.
     * <p>
     * The option starts a consistent read for storage engines such as InnoDB and XtraDB that can do so, the
     * same as if a {@code START TRANSACTION} followed by a {@code SELECT ...} from any InnoDB table was
     * issued.
     */
    Option<Boolean> WITH_CONSISTENT_SNAPSHOT = Option.valueOf("withConsistentSnapshot");

    /**
     * Use {@code WITH CONSISTENT [engine] SNAPSHOT} for Facebook/MySQL or similar property. Only available
     * when {@link #WITH_CONSISTENT_SNAPSHOT} is set to {@code true}.
     * <p>
     * Note: This is an extended syntax based on specific distributions. Please check whether the server
     * supports this property before using it.
     */
    Option<String> CONSISTENT_SNAPSHOT_ENGINE = Option.valueOf("consistentSnapshotEngine");

    /**
     * Use {@code WITH CONSISTENT SNAPSHOT FROM SESSION [session_id]} for Percona/MySQL or similar property.
     * Only available when {@link #WITH_CONSISTENT_SNAPSHOT} is set to {@code true}.
     * <p>
     * The {@code session_id} is received by {@code SHOW COLUMNS FROM performance_schema.processlist}, it
     * should be an unsigned 64-bit integer. Use {@code SHOW PROCESSLIST} to find session identifier of the
     * process list.
     * <p>
     * Note: This is an extended syntax based on specific distributions. Please check whether the server
     * supports this property before using it.
     */
    Option<Long> CONSISTENT_SNAPSHOT_FROM_SESSION = Option.valueOf("consistentSnapshotFromSession");

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and applying
     * {@link IsolationLevel}.
     *
     * @param isolationLevel the isolation level to use during the transaction.
     * @return a new {@link MySqlTransactionDefinition} with the {@code isolationLevel}.
     * @throws IllegalArgumentException if {@code isolationLevel} is {@code null}.
     */
    MySqlTransactionDefinition isolationLevel(IsolationLevel isolationLevel);

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and using the default
     * isolation level. Removes transaction isolation level if configured already.
     *
     * @return a new {@link MySqlTransactionDefinition} without specified isolation level.
     */
    MySqlTransactionDefinition withoutIsolationLevel();

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and using read-only
     * transaction semantics. Overrides transaction mutability if configured already.
     *
     * @return a new {@link MySqlTransactionDefinition} with read-only semantics.
     */
    MySqlTransactionDefinition readOnly();

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and using explicitly
     * read-write transaction semantics. Overrides transaction mutability if configured already.
     *
     * @return a new {@link MySqlTransactionDefinition} with read-write semantics.
     */
    MySqlTransactionDefinition readWrite();

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and avoid to using
     * explicitly mutability. Removes transaction mutability if configured already.
     *
     * @return a new {@link MySqlTransactionDefinition} without explicitly mutability.
     */
    MySqlTransactionDefinition withoutMutability();

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and applying a lock wait
     * timeout. Overrides transaction lock wait timeout if configured already.
     * <p>
     * Note: for now, it is only available in InnoDB or InnoDB-compatible engines.
     *
     * @param timeout the lock wait timeout.
     * @return a new {@link MySqlTransactionDefinition} with the {@code timeout}.
     * @throws IllegalArgumentException if {@code timeout} is {@code null}.
     */
    MySqlTransactionDefinition lockWaitTimeout(Duration timeout);

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and applying to use the
     * default lock wait timeout. Removes transaction lock wait timeout if configured already.
     *
     * @return a new {@link MySqlTransactionDefinition} without specified lock wait timeout.
     */
    MySqlTransactionDefinition withoutLockWaitTimeout();

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and applying to with
     * consistent snapshot. Overrides transaction consistency if configured already.
     *
     * @return a new {@link MySqlTransactionDefinition} with consistent snapshot semantics.
     */
    MySqlTransactionDefinition consistent();

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and applying to with
     * consistent engine snapshot. Overrides transaction consistency if configured already.
     *
     * @param engine the consistent snapshot engine, e.g. {@code ROCKSDB}.
     * @return a new {@link MySqlTransactionDefinition} with consistent snapshot semantics.
     * @throws IllegalArgumentException if {@code engine} is {@code null}.
     */
    MySqlTransactionDefinition consistent(String engine);

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and applying to with
     * consistent engine snapshot from session. Overrides transaction consistency if configured already.
     *
     * @param engine    the consistent snapshot engine, e.g. {@code ROCKSDB}.
     * @param sessionId the session id.
     * @return a new {@link MySqlTransactionDefinition} with consistent snapshot semantics.
     * @throws IllegalArgumentException if {@code engine} is {@code null}.
     */
    MySqlTransactionDefinition consistent(String engine, long sessionId);

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and applying to with
     * consistent snapshot from session. Overrides transaction consistency if configured already.
     *
     * @param sessionId the session id.
     * @return a new {@link MySqlTransactionDefinition} with consistent snapshot semantics.
     */
    MySqlTransactionDefinition consistentFromSession(long sessionId);

    /**
     * Creates a {@link MySqlTransactionDefinition} retaining all configured options and applying to without
     * consistent snapshot. Removes transaction consistency if configured already.
     *
     * @return a new {@link MySqlTransactionDefinition} without consistent snapshot semantics.
     */
    MySqlTransactionDefinition withoutConsistent();

    /**
     * Gets an empty {@link MySqlTransactionDefinition}.
     *
     * @return an empty {@link MySqlTransactionDefinition}.
     */
    static MySqlTransactionDefinition empty() {
        return SimpleTransactionDefinition.EMPTY;
    }

    /**
     * Creates a {@link MySqlTransactionDefinition} specifying transaction mutability.
     *
     * @param readWrite {@code true} for read-write, {@code false} to use a read-only transaction.
     * @return a new {@link MySqlTransactionDefinition} using the specified transaction mutability.
     */
    static MySqlTransactionDefinition mutability(boolean readWrite) {
        return readWrite ? SimpleTransactionDefinition.EMPTY.readWrite() :
            SimpleTransactionDefinition.EMPTY.readOnly();
    }

    static MySqlTransactionDefinition from(IsolationLevel isolationLevel) {
        return SimpleTransactionDefinition.EMPTY.isolationLevel(isolationLevel);
    }
}

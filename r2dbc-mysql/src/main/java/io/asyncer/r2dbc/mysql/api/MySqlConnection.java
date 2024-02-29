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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Lifecycle;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * A {@link Connection} for connecting to a MySQL database.
 *
 * @since 1.1.3
 */
public interface MySqlConnection extends Connection, Lifecycle {

    /**
     * {@inheritDoc}
     * <p>
     * Note: MySQL server will disable the auto-commit mode automatically when a transaction is started.
     *
     * @return a {@link Mono} that indicates that the transaction has begun
     */
    @Override
    Mono<Void> beginTransaction();

    /**
     * {@inheritDoc}
     * <p>
     * Note: MySQL server will disable the auto-commit mode automatically when a transaction is started.
     *
     * @param definition the transaction definition, must not be {@code null}
     * @return a {@link Mono} that indicates that the transaction has begun
     * @throws IllegalArgumentException if {@code definition} is {@code null}
     */
    @Override
    Mono<Void> beginTransaction(TransactionDefinition definition);

    /**
     * {@inheritDoc}
     *
     * @return a {@link Mono} that indicates that the connection has been closed
     */
    @Override
    Mono<Void> close();

    /**
     * {@inheritDoc}
     *
     * @return a {@link Mono} that indicates that the transaction has been committed
     */
    @Override
    Mono<Void> commitTransaction();

    /**
     * {@inheritDoc}
     *
     * @return a {@link MySqlBatch} that can be used to execute a batch of statements
     */
    @Override
    MySqlBatch createBatch();

    /**
     * {@inheritDoc}
     *
     * @param name the savepoint name, must not be {@code null}
     * @return a {@link Mono} that indicates that the savepoint has been created
     * @throws IllegalArgumentException if {@code name} is {@code null}
     */
    @Override
    Mono<Void> createSavepoint(String name);

    /**
     * {@inheritDoc}
     *
     * @param sql the SQL to execute, must not be {@code null}
     * @return a new {@link MySqlStatement} instance
     * @throws IllegalArgumentException if {@code sql} is {@code null}
     */
    @Override
    MySqlStatement createStatement(String sql);

    /**
     * {@inheritDoc}
     *
     * @return a {@link MySqlConnectionMetadata} that contains the connection metadata
     */
    @Override
    MySqlConnectionMetadata getMetadata();

    /**
     * {@inheritDoc}
     *
     * @param name the savepoint name, must not be {@code null}
     * @return a {@link Mono} that indicates that the savepoint has been released
     * @throws IllegalArgumentException if {@code name} is {@code null}
     */
    @Override
    Mono<Void> releaseSavepoint(String name);

    /**
     * {@inheritDoc}
     *
     * @return a {@link Mono} that indicates that the transaction has been rolled back
     */
    @Override
    Mono<Void> rollbackTransaction();

    /**
     * {@inheritDoc}
     *
     * @param name the savepoint name, must not be {@code null}
     * @return a {@link Mono} that indicates that the transaction has been rolled back to the savepoint
     * @throws IllegalArgumentException if {@code name} is {@code null}
     */
    @Override
    Mono<Void> rollbackTransactionToSavepoint(String name);

    /**
     * {@inheritDoc}
     *
     * @param autoCommit the auto-commit mode
     * @return a {@link Mono} that indicates that the auto-commit mode has been set
     */
    @Override
    Mono<Void> setAutoCommit(boolean autoCommit);

    /**
     * {@inheritDoc}
     * <p>
     * Note: Currently, it should be used only for InnoDB storage engine.
     *
     * @param timeout the lock wait timeout, must not be {@code null}
     * @return a {@link Mono} that indicates that the lock wait timeout has been set
     * @throws IllegalArgumentException if {@code timeout} is {@code null}
     */
    @Override
    Mono<Void> setLockWaitTimeout(Duration timeout);

    /**
     * {@inheritDoc}
     *
     * @param timeout the statement timeout, must not be {@code null}
     * @return a {@link Mono} that indicates that the statement timeout has been set
     * @throws IllegalArgumentException if {@code timeout} is {@code null}
     */
    @Override
    Mono<Void> setStatementTimeout(Duration timeout);

    /**
     * {@inheritDoc}
     *
     * @param isolationLevel the isolation level, must not be {@code null}
     * @return a {@link Mono} that indicates that the isolation level of the current session has been set
     * @throws IllegalArgumentException if {@code isolationLevel} is {@code null}
     */
    @Override
    Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel);

    /**
     * {@inheritDoc}
     *
     * @param depth the validation depth, must not be {@code null}
     * @return a {@link Mono} that indicates that the connection has been validated
     * @throws IllegalArgumentException if {@code depth} is {@code null}
     */
    @Override
    Mono<Boolean> validate(ValidationDepth depth);

    /**
     * {@inheritDoc}
     *
     * @return a {@link Mono} that indicates that the connection is ready for usage
     */
    @Override
    Mono<Void> postAllocate();

    /**
     * {@inheritDoc}
     *
     * @return a {@link Mono} that indicates that the connection is ready for release
     */
    @Override
    Mono<Void> preRelease();
}

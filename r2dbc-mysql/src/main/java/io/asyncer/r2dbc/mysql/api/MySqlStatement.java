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

import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;

import java.util.NoSuchElementException;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * A strongly typed abstraction of {@link Statement} for a SQL statement against a MySQL database.
 *
 * @since 1.1.3
 */
public interface MySqlStatement extends Statement {

    /**
     * {@inheritDoc}
     *
     * @return {@link MySqlStatement this}
     * @throws IllegalStateException if the statement is parametrized and not all parameters are provided
     */
    @Override
    MySqlStatement add();

    /**
     * {@inheritDoc}
     *
     * @param index the index to bind to
     * @param value the value to bind
     * @return {@link MySqlStatement this}
     * @throws IllegalArgumentException      if {@code value} is {@code null}
     * @throws IndexOutOfBoundsException     if the parameter {@code index} is out of range
     * @throws UnsupportedOperationException if the statement is not a parameterized statement
     */
    @Override
    MySqlStatement bind(int index, Object value);

    /**
     * {@inheritDoc}
     *
     * @param name  the name of identifier to bind to
     * @param value the value to bind
     * @return {@link MySqlStatement this}
     * @throws IllegalArgumentException      if {@code name} or {@code value} is {@code null}
     * @throws NoSuchElementException        if {@code name} is not a known name to bind
     * @throws UnsupportedOperationException if the statement is not a parameterized statement
     */
    @Override
    MySqlStatement bind(String name, Object value);

    /**
     * {@inheritDoc}
     *
     * @param index the index to bind to
     * @param type  the type of null value
     * @return {@link MySqlStatement this}
     * @throws IllegalArgumentException      if {@code type} is {@code null}
     * @throws IndexOutOfBoundsException     if the parameter {@code index} is out of range
     * @throws UnsupportedOperationException if the statement is not a parameterized statement
     */
    @Override
    MySqlStatement bindNull(int index, Class<?> type);

    /**
     * {@inheritDoc}
     *
     * @param name the name of identifier to bind to
     * @param type the type of null value
     * @return {@link MySqlStatement this}
     * @throws IllegalArgumentException      if {@code name} or {@code type} is {@code null}
     * @throws NoSuchElementException        if {@code name} is not a known name to bind
     * @throws UnsupportedOperationException if the statement is not a parameterized statement
     */
    @Override
    MySqlStatement bindNull(String name, Class<?> type);

    /**
     * {@inheritDoc}
     *
     * @return a {@link Flux} representing {@link MySqlResult}s of the statement
     * @throws IllegalStateException if the statement is parametrized and not all parameters are provided
     */
    @Override
    Flux<? extends MySqlResult> execute();

    /**
     * {@inheritDoc}
     *
     * @param columns the names of the columns to return
     * @return {@link MySqlStatement this}
     * @throws IllegalArgumentException if {@code columns}, or any item is empty or {@code null}
     */
    @Override
    default MySqlStatement returnGeneratedValues(String... columns) {
        requireNonNull(columns, "columns must not be null");
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * @param rows the number of rows to fetch
     * @return {@link MySqlStatement this}
     * @throws IllegalArgumentException if fetch size is less than zero
     */
    @Override
    default MySqlStatement fetchSize(int rows) {
        require(rows >= 0, "Fetch size must be greater or equal to zero");
        return this;
    }
}

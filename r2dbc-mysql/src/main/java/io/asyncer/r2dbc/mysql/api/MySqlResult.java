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

import io.r2dbc.spi.OutParameters;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A {@link Result} for results of a query against a MySQL database.
 * <p>
 * Note: A query may return multiple {@link MySqlResult}s.
 *
 * @since 1.1.3
 */
public interface MySqlResult extends Result {

    /**
     * {@inheritDoc}
     *
     * @return a {@link Mono} emitting the number of rows updated, or empty if it is not an update result.
     * @throws IllegalStateException if the result was consumed
     */
    @Override
    Mono<Long> getRowsUpdated();

    /**
     * {@inheritDoc}
     *
     * @param mappingFunction that maps a {@link Row} and {@link RowMetadata} to a value
     * @param <T>             the type of the mapped value
     * @return a {@link Flux} of mapped results
     * @throws IllegalArgumentException if {@code mappingFunction} is {@code null}
     * @throws IllegalStateException    if the result was consumed
     */
    @Override
    <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction);

    /**
     * {@inheritDoc}
     *
     * @param mappingFunction that maps a {@link Readable} to a value
     * @param <T>             the type of the mapped value
     * @return a {@link Flux} of mapped results
     * @throws IllegalArgumentException if {@code mappingFunction} is {@code null}
     * @throws IllegalStateException    if the result was consumed
     * @see MySqlReadable
     * @see MySqlRow
     * @see MySqlOutParameters
     */
    @Override
    <T> Flux<T> map(Function<? super Readable, ? extends T> mappingFunction);

    /**
     * {@inheritDoc}
     *
     * @param filter to apply to each element to determine if it should be included
     * @return a {@link MySqlResult} that will only emit results that match the {@code predicate}
     * @throws IllegalArgumentException if {@code predicate} is {@code null}
     * @throws IllegalStateException    if the result was consumed
     */
    @Override
    MySqlResult filter(Predicate<Result.Segment> filter);

    /**
     * {@inheritDoc}
     *
     * @param mappingFunction that maps a {@link Result.Segment} a to a {@link Publisher}
     * @param <T>             the type of the mapped value
     * @return a {@link Flux} of mapped results
     * @throws IllegalArgumentException if {@code mappingFunction} is {@code null}
     * @throws IllegalStateException    if the result was consumed
     */
    @Override
    <T> Flux<T> flatMap(Function<Result.Segment, ? extends Publisher<? extends T>> mappingFunction);

    /**
     * Marker interface for a MySQL result segment. Result segments represent the individual parts of a result
     * from a query against a MySQL database. It is a sealed interface.
     *
     * @see RowSegment
     * @see OutSegment
     * @see UpdateCount
     * @see Message
     * @see OkSegment
     */
    interface Segment extends Result.Segment {
    }

    /**
     * Row segment consisting of {@link Row row data}.
     */
    interface RowSegment extends Segment, Result.RowSegment {

        /**
         * Gets the {@link MySqlRow row data}.
         *
         * @return a {@link MySqlRow} of data
         */
        @Override
        MySqlRow row();
    }

    /**
     * Out parameters segment consisting of {@link OutParameters readable data}.
     */
    interface OutSegment extends Segment, Result.OutSegment {

        /**
         * Retrieve all {@code OUT} parameters as a {@link MySqlRow}.
         * <p>
         * In MySQL, {@code OUT} parameters are returned as a row. These rows will be preceded by a flag
         * indicating that the following rows are {@code OUT} parameters. So, an {@link OutSegment} must
         * can be retrieved as a {@link MySqlRow}, but not vice versa.
         *
         * @return a {@link MySqlRow} of all {@code OUT} parameters
         */
        MySqlRow row();

        /**
         * Gets all {@link OutParameters OUT parameters}.
         *
         * @return a {@link OutParameters} of data
         */
        @Override
        MySqlOutParameters outParameters();
    }

    /**
     * Update count segment consisting providing an {@link #value() affected rows count}.
     */
    interface UpdateCount extends Segment, Result.UpdateCount {
    }

    /**
     * Message segment reported as result of the statement processing.
     */
    interface Message extends Segment, Result.Message {
    }

    /**
     * Insert result segment consisting of a {@link #row() last inserted id} and
     * {@link #value() affected rows count}, and only appears if the statement is an insert, the table has an
     * auto-increment identifier column, and the statement is not using the {@code RETURNING} clause.
     * <p>
     * Note: a {@link MySqlResult} will return only the last inserted id whatever how many rows are inserted.
     */
    interface OkSegment extends RowSegment, UpdateCount {
    }
}

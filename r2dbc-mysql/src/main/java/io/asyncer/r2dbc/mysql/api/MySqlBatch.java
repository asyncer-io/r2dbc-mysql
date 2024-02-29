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

import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

/**
 * {@link Batch} for executing a collection of statements in a batch against a MySQL database.
 *
 * @since 1.1.3
 */
public interface MySqlBatch extends Batch {

    /**
     * {@inheritDoc}
     *
     * @param sql the statement to add
     * @return {@link MySqlBatch this}
     * @throws IllegalArgumentException if {@code sql} is {@code null}
     */
    @Override
    MySqlBatch add(String sql);

    /**
     * {@inheritDoc}
     *
     * @return the {@link MySqlResult}s of executing the batch
     */
    @Override
    Flux<? extends MySqlResult> execute();
}

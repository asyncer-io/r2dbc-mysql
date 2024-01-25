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

import io.asyncer.r2dbc.mysql.codec.Codecs;
import reactor.core.publisher.Flux;

/**
 * An implementation of {@link MySqlStatement} considers the lightweight ping syntax.
 */
final class PingStatement implements MySqlStatement {

    private final MySqlConnection connection;

    private final Codecs codecs;

    private final ConnectionContext context;

    PingStatement(MySqlConnection connection, Codecs codecs, ConnectionContext context) {
        this.connection = connection;
        this.codecs = codecs;
        this.context = context;
    }

    @Override
    public MySqlStatement add() {
        return this;
    }

    @Override
    public MySqlStatement bind(int index, Object value) {
        throw new UnsupportedOperationException("Binding parameters is not supported for ping");
    }

    @Override
    public MySqlStatement bind(String name, Object value) {
        throw new UnsupportedOperationException("Binding parameters is not supported for ping");
    }

    @Override
    public MySqlStatement bindNull(int index, Class<?> type) {
        throw new UnsupportedOperationException("Binding parameters is not supported for ping");
    }

    @Override
    public MySqlStatement bindNull(String name, Class<?> type) {
        throw new UnsupportedOperationException("Binding parameters is not supported for ping");
    }

    @Override
    public Flux<MySqlResult> execute() {
        return Flux.just(MySqlResult.toResult(false, codecs, context, null,
            connection.doPingInternal()));
    }
}

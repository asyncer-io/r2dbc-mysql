/*
 * Copyright 2023 asyncer.io projects
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

import io.asyncer.r2dbc.mysql.api.MySqlBatch;
import io.asyncer.r2dbc.mysql.api.MySqlResult;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlBatch} for executing a collection of statements in one-by-one against the
 * MySQL database.
 */
final class MySqlSyntheticBatch implements MySqlBatch {

    private final Client client;

    private final Codecs codecs;

    private final List<String> statements = new ArrayList<>();

    MySqlSyntheticBatch(Client client, Codecs codecs) {
        this.client = requireNonNull(client, "client must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
    }

    @Override
    public MySqlBatch add(String sql) {
        statements.add(requireNonNull(sql, "sql must not be null"));
        return this;
    }

    @Override
    public Flux<MySqlResult> execute() {
        return QueryFlow.execute(client, statements)
            .map(messages -> MySqlSegmentResult.toResult(false, client, codecs, null, messages));
    }

    @Override
    public String toString() {
        return "MySqlSyntheticBatch{sql=REDACTED}";
    }
}

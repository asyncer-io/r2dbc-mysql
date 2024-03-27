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

import io.asyncer.r2dbc.mysql.api.MySqlResult;
import io.asyncer.r2dbc.mysql.api.MySqlStatement;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import reactor.core.publisher.Flux;

import java.util.List;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;

/**
 * An implementation of {@link ParameterizedStatementSupport} based on MySQL prepare query.
 */
final class PrepareParameterizedStatement extends ParameterizedStatementSupport {

    private int fetchSize = 0;

    PrepareParameterizedStatement(Client client, Codecs codecs, Query query) {
        super(client, codecs, query);
    }

    @Override
    public Flux<MySqlResult> execute(List<Binding> bindings) {
        return Flux.defer(() -> QueryFlow.execute(client,
                StringUtils.extendReturning(query.getFormattedSql(), returningIdentifiers()),
                bindings, fetchSize
            ))
            .map(messages -> MySqlSegmentResult.toResult(true, client, codecs, syntheticKeyName(), messages));
    }

    @Override
    public MySqlStatement fetchSize(int rows) {
        require(rows >= 0, "Fetch size must be greater or equal to zero");

        this.fetchSize = rows;
        return this;
    }
}

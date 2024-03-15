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
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * An implementation of {@link ParametrizedStatementSupport} based on MySQL text query.
 */
final class TextParametrizedStatement extends ParametrizedStatementSupport {

    TextParametrizedStatement(Client client, Codecs codecs, Query query) {
        super(client, codecs, query);
    }

    @Override
    protected Flux<MySqlResult> execute(List<Binding> bindings) {
        return Flux.defer(() -> QueryFlow.execute(client, query, returningIdentifiers(), bindings))
            .map(messages -> MySqlSegmentResult.toResult(false, client, codecs, syntheticKeyName(), messages));
    }
}

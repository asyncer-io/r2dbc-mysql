/*
 * Copyright 2026 asyncer.io projects
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

import io.asyncer.r2dbc.mysql.client.FluxExchangeable;
import io.asyncer.r2dbc.mysql.message.server.ServerMessage;

/**
 * Exposes package-private {@link FluxExchangeable} implementations to tests in other packages.
 */
public final class ExchangeableTestSupport {

    public static FluxExchangeable<ServerMessage> simpleQuery(String sql) {
        return new SimpleQueryExchangeable(sql);
    }

    private ExchangeableTestSupport() {
    }
}

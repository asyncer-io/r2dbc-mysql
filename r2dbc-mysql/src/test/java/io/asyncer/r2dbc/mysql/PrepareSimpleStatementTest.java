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

import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.codec.Codecs;

import java.lang.reflect.Field;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PrepareSimpleStatement}.
 */
class PrepareSimpleStatementTest implements StatementTestSupport<PrepareSimpleStatement> {

    private final Codecs codecs = mock(Codecs.class);

    private final Field fetchSize = PrepareSimpleStatement.class.getDeclaredField("fetchSize");

    PrepareSimpleStatementTest() throws NoSuchFieldException {
        fetchSize.setAccessible(true);
    }

    @Override
    public void badAdd() {
        // No-op
    }

    @Override
    public void bind() {
        // No-op
    }

    @Override
    public void bindNull() {
        // No-op
    }

    @Override
    public int getFetchSize(PrepareSimpleStatement statement) throws IllegalAccessException {
        return fetchSize.getInt(statement);
    }

    @Override
    public PrepareSimpleStatement makeInstance(boolean isMariaDB, String ignored, String sql) {
        Client client = mock(Client.class);

        when(client.getContext()).thenReturn(ConnectionContextTest.mock(isMariaDB));

        return new PrepareSimpleStatement(client, codecs, sql);
    }

    @Override
    public boolean supportsBinding() {
        return false;
    }
}

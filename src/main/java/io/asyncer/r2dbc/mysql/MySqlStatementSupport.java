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


import org.jetbrains.annotations.Nullable;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireValidName;

/**
 * Base class considers generic logic for {@link MySqlStatement} implementations.
 */
abstract class MySqlStatementSupport implements MySqlStatement {

    private static final String LAST_INSERT_ID = "LAST_INSERT_ID";

    @Nullable
    String generatedKeyName = null;

    @Override
    public final MySqlStatement returnGeneratedValues(String... columns) {
        requireNonNull(columns, "columns must not be null");

        switch (columns.length) {
            case 0:
                this.generatedKeyName = LAST_INSERT_ID;
                return this;
            case 1:
                this.generatedKeyName = requireValidName(columns[0],
                    "id name must not be empty and not contain backticks");
                return this;
        }

        throw new IllegalArgumentException("MySQL only supports single generated value");
    }

    @Override
    public MySqlStatement fetchSize(int rows) {
        require(rows >= 0, "Fetch size must be greater or equal to zero");
        return this;
    }
}

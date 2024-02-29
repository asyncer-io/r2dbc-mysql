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

import io.asyncer.r2dbc.mysql.api.MySqlConnectionMetadata;
import org.jetbrains.annotations.Nullable;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * Connection metadata for a connection connected to MySQL database.
 */
final class MySqlSimpleConnectionMetadata implements MySqlConnectionMetadata {

    private final String version;

    private final String product;

    private final boolean isMariaDb;

    MySqlSimpleConnectionMetadata(String version, @Nullable String product, boolean isMariaDb) {
        this.version = requireNonNull(version, "version must not be null");
        this.product = product == null ? "Unknown" : product;
        this.isMariaDb = isMariaDb;
    }

    @Override
    public String getDatabaseVersion() {
        return version;
    }

    @Override
    public boolean isMariaDb() {
        return isMariaDb;
    }

    @Override
    public String getDatabaseProductName() {
        return product;
    }
}

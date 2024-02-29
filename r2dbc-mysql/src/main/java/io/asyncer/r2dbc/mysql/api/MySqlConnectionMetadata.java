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

import io.r2dbc.spi.ConnectionMetadata;

/**
 * {@link ConnectionMetadata} for a connection connected to a MySQL database.
 *
 * @since 1.1.3
 */
public interface MySqlConnectionMetadata extends ConnectionMetadata {

    /**
     * {@inheritDoc}
     * <p>
     * Note: it should be the result of {@code SELECT @@version_comment}
     *
     * @return the product name of the database
     */
    @Override
    String getDatabaseProductName();

    /**
     * {@inheritDoc}
     *
     * @return the version received from the server, e.g. {@code 5.7.30}, {@code 5.5.5-10.4.13-MariaDB}
     */
    @Override
    String getDatabaseVersion();

    /**
     * Checks if the connection is in MariaDB mode.
     *
     * @return {@code true} if it is MariaDB
     */
    boolean isMariaDb();
}

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

import io.r2dbc.spi.RowMetadata;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * {@link RowMetadata} for a row metadata returned from a MySQL database.
 *
 * @since 1.1.3
 */
public interface MySqlRowMetadata extends RowMetadata {

    /**
     * {@inheritDoc}
     *
     * @param index the column index starting at 0
     * @return the {@link MySqlRowMetadata} for one column in this row
     * @throws IndexOutOfBoundsException if {@code index} is out of range
     */
    @Override
    MySqlColumnMetadata getColumnMetadata(int index);

    /**
     * {@inheritDoc}
     *
     * @param name the name of the column.  Column names are case-insensitive.  When a get method contains
     *             several columns with same name, then the value of the first matching column will be
     *             returned
     * @return the {@link MySqlColumnMetadata} for one column in this row
     * @throws IllegalArgumentException if {@code name} is {@code null}
     * @throws NoSuchElementException   if there is no column with the {@code name}
     */
    @Override
    MySqlColumnMetadata getColumnMetadata(String name);

    /**
     * {@inheritDoc}
     *
     * @return the {@link MySqlColumnMetadata} for all columns in this row
     */
    @Override
    List<? extends MySqlColumnMetadata> getColumnMetadatas();

    /**
     * {@inheritDoc}
     *
     * @param columnName the name of the column.  Column names are case-insensitive.
     * @return {@code true} if this object contains metadata for {@code columnName}; {@code false} otherwise.
     */
    @Override
    boolean contains(String columnName);
}

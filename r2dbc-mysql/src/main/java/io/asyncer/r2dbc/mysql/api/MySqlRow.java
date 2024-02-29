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

import io.r2dbc.spi.Row;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.util.NoSuchElementException;

/**
 * A {@link Row} for a data row of a {@link MySqlResult}.
 *
 * @since 1.1.3
 */
public interface MySqlRow extends MySqlReadable, Row {

    /**
     * Returns the {@link MySqlRowMetadata} for all columns in this row.
     *
     * @return the {@link MySqlRowMetadata} for all columns in this row
     */
    @Override
    MySqlRowMetadata getMetadata();

    /**
     * Returns the value which can be a generic type.
     * <p>
     * UNSTABLE: it is not a standard of {@code r2dbc-spi}, so it may be changed in the future.
     *
     * @param index the index starting at {@code 0}
     * @param type  the parameterized type of item to return.
     * @param <T>   the type of the item being returned.
     * @return the value for a column in this row. Value can be {@code null}.
     * @throws IllegalArgumentException      if {@code name} or {@code type} is {@code null}.
     * @throws IndexOutOfBoundsException     if {@code index} is out of range
     * @throws UnsupportedOperationException if the row is containing last inserted ID
     */
    @Nullable <T> T get(int index, ParameterizedType type);

    /**
     * Returns the value which can be a generic type.
     * <p>
     * UNSTABLE: it is not a standard of {@code r2dbc-spi}, so it may be changed in the future.
     *
     * @param name the name of the column.
     * @param type the parameterized type of item to return.
     * @param <T>  the type of the item being returned.
     * @return the value for a column in this row. Value can be {@code null}.
     * @throws IllegalArgumentException      if {@code name} or {@code type} is {@code null}.
     * @throws NoSuchElementException        if {@code name} is not a known readable column or out parameter
     * @throws UnsupportedOperationException if the row is containing last inserted ID
     */
    @Nullable <T> T get(String name, ParameterizedType type);
}

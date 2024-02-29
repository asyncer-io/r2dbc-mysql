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

import io.r2dbc.spi.OutParametersMetadata;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * {@link OutParametersMetadata} for {@code OUT} parameters metadata returned from a MySQL database.
 *
 * @since 1.1.3
 */
public interface MySqlOutParametersMetadata extends OutParametersMetadata {

    /**
     * {@inheritDoc}
     *
     * @param index the out parameter index starting at 0
     * @return the {@link MySqlOutParametersMetadata} for one out parameter
     * @throws IndexOutOfBoundsException if {@code index} is out of range
     */
    @Override
    MySqlOutParameterMetadata getParameterMetadata(int index);

    /**
     * {@inheritDoc}
     *
     * @param name the name of the out parameter.  Parameter names are case-insensitive.
     * @return the {@link MySqlOutParameterMetadata} for one out parameter
     * @throws IllegalArgumentException if {@code name} is {@code null}
     * @throws NoSuchElementException   if there is no out parameter with the {@code name}
     */
    @Override
    MySqlOutParameterMetadata getParameterMetadata(String name);

    /**
     * {@inheritDoc}
     *
     * @return the {@link MySqlOutParameterMetadata} for all out parameters
     */
    @Override
    List<? extends MySqlOutParameterMetadata> getParameterMetadatas();
}

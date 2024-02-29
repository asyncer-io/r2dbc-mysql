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

import io.asyncer.r2dbc.mysql.codec.CodecContext;
import io.asyncer.r2dbc.mysql.collation.CharCollation;
import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.r2dbc.spi.ReadableMetadata;

/**
 * {@link ReadableMetadata} for metadata of a column or an {@code OUT} parameter returned from a MySQL
 * database.
 *
 * @since 1.1.3
 */
public interface MySqlReadableMetadata extends ReadableMetadata {

    /**
     * {@inheritDoc}
     *
     * @return the {@link MySqlType} descriptor.
     */
    @Override
    MySqlType getType();

    /**
     * Gets the {@link CharCollation} used for stringification type.  If server-side collation is binary, it
     * will return the default client collation of {@code context}.
     *
     * @param context the codec context for load the default character collation.
     * @return the {@link CharCollation}.
     */
    CharCollation getCharCollation(CodecContext context);

    /**
     * {@inheritDoc}
     *
     * @return the {@link MySqlNativeTypeMetadata}.
     */
    @Override
    default MySqlNativeTypeMetadata getNativeTypeMetadata() {
        return null;
    }

    /**
     * {@inheritDoc}
     *
     * @return the primary Java {@link Class type}.
     * @see MySqlRow#get
     * @see MySqlOutParameters#get
     */
    @Override
    default Class<?> getJavaType() {
        return getType().getJavaType();
    }
}

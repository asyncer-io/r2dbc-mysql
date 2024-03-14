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

import io.asyncer.r2dbc.mysql.api.MySqlRow;
import io.asyncer.r2dbc.mysql.api.MySqlRowMetadata;
import io.asyncer.r2dbc.mysql.codec.CodecContext;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import io.asyncer.r2dbc.mysql.message.FieldValue;
import io.r2dbc.spi.Row;

import java.lang.reflect.ParameterizedType;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link Row} for MySQL database.
 */
final class MySqlDataRow implements MySqlRow {

    private final FieldValue[] fields;

    private final MySqlRowDescriptor rowMetadata;

    private final Codecs codecs;

    /**
     * It is binary decode logic.
     */
    private final boolean binary;

    /**
     * It can be retained because it is provided by the executed connection instead of the current connection.
     */
    private final CodecContext context;

    MySqlDataRow(FieldValue[] fields, MySqlRowDescriptor rowMetadata, Codecs codecs, boolean binary,
        CodecContext context) {
        this.fields = requireNonNull(fields, "fields must not be null");
        this.rowMetadata = requireNonNull(rowMetadata, "rowMetadata must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
        this.binary = binary;
        this.context = requireNonNull(context, "context must not be null");
    }

    @Override
    public <T> T get(int index, Class<T> type) {
        requireNonNull(type, "type must not be null");

        MySqlColumnDescriptor info = rowMetadata.getColumnMetadata(index);
        return codecs.decode(fields[index], info, type, binary, context);
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        requireNonNull(type, "type must not be null");

        MySqlColumnDescriptor info = rowMetadata.getColumnMetadata(name);
        return codecs.decode(fields[info.getIndex()], info, type, binary, context);
    }

    @Override
    public <T> T get(int index, ParameterizedType type) {
        requireNonNull(type, "type must not be null");

        MySqlColumnDescriptor info = rowMetadata.getColumnMetadata(index);
        return codecs.decode(fields[index], info, type, binary, context);
    }

    @Override
    public <T> T get(String name, ParameterizedType type) {
        requireNonNull(type, "type must not be null");

        MySqlColumnDescriptor info = rowMetadata.getColumnMetadata(name);
        return codecs.decode(fields[info.getIndex()], info, type, binary, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MySqlRowMetadata getMetadata() {
        return rowMetadata;
    }
}

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

import io.asyncer.r2dbc.mysql.api.MySqlRowMetadata;
import io.asyncer.r2dbc.mysql.internal.util.InternalArrays;
import io.asyncer.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlRowMetadata} for MySQL database text/binary results.
 */
final class MySqlRowDescriptor implements MySqlRowMetadata {

    private final MySqlColumnDescriptor[] originMetadata;

    @Nullable
    private Map<String, Integer> indexMap;

    /**
     * Visible for testing
     */
    @VisibleForTesting
    MySqlRowDescriptor(MySqlColumnDescriptor[] metadata) {
        originMetadata = metadata;
    }

    @Override
    public MySqlColumnDescriptor getColumnMetadata(int index) {
        if (index < 0 || index >= originMetadata.length) {
            throw new ArrayIndexOutOfBoundsException("Index: " + index + ", total: " + originMetadata.length);
        }

        return originMetadata[index];
    }

    private static Map<String, Integer> createIndexMap(MySqlColumnDescriptor[] metadata) {
        final int size = metadata.length;
        final Map<String, Integer> map = new HashMap<>(size);

        for (int i = 0; i < size; ++i) {
            map.putIfAbsent(metadata[i].getName().toLowerCase(Locale.ROOT), i);
        }

        return map;
    }

    private int find(final String name) {
        Map<String, Integer> indexMap = this.indexMap;
        if (null == indexMap) {
            indexMap = this.indexMap = createIndexMap(originMetadata);
        }
        return indexMap.getOrDefault(name.toLowerCase(Locale.ROOT), -1);
    }

    @Override
    public MySqlColumnDescriptor getColumnMetadata(String name) {
        requireNonNull(name, "name must not be null");

        final int index = find(name);

        if (index < 0) {
            throw new NoSuchElementException("Column name '" + name + "' does not exist");
        }

        return originMetadata[index];
    }

    @Override
    public boolean contains(String name) {
        requireNonNull(name, "name must not be null");

        return find(name) >= 0;
    }

    @Override
    public List<MySqlColumnDescriptor> getColumnMetadatas() {
        return InternalArrays.asImmutableList(originMetadata);
    }

    @Override
    public String toString() {
        return "MySqlRowDescriptor{metadata=" + Arrays.toString(originMetadata) + '}';
    }

    MySqlColumnDescriptor[] unwrap() {
        return originMetadata;
    }

    static MySqlRowDescriptor create(DefinitionMetadataMessage[] columns) {
        int size = columns.length;
        MySqlColumnDescriptor[] metadata = new MySqlColumnDescriptor[size];

        for (int i = 0; i < size; ++i) {
            metadata[i] = MySqlColumnDescriptor.create(i, columns[i]);
        }

        return new MySqlRowDescriptor(metadata);
    }
}

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

import io.asyncer.r2dbc.mysql.internal.util.InternalArrays;
import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import io.asyncer.r2dbc.mysql.message.server.DefinitionMetadataMessage;
import io.r2dbc.spi.RowMetadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link RowMetadata} for MySQL database text/binary results.
 *
 * @see MySqlNames column name searching rules.
 */
final class MySqlRowMetadata implements RowMetadata {

    private final MySqlColumnDescriptor[] originMetadata;

    private Map<String, Integer> nameIndexMap;

    private MySqlRowMetadata(MySqlColumnDescriptor[] metadata) {
        this.originMetadata = metadata;
    }

    @Override
    public MySqlColumnDescriptor getColumnMetadata(int index) {
        if (index < 0 || index >= originMetadata.length) {
            throw new ArrayIndexOutOfBoundsException("Index: " + index + ", total: " + originMetadata.length);
        }

        return originMetadata[index];
    }

    private static Map<String, Integer> createNameIndexMap(final MySqlColumnDescriptor[] metadata) {
        final int size = metadata.length;
        final Map<String, Integer> map = new HashMap<>(size * 2);
        for (int i = 0; i < size; ++i) {
            map.putIfAbsent(metadata[i].getName(), i);
            map.putIfAbsent(metadata[i].getName().toLowerCase(Locale.ROOT), i);
        }
        return map;
    }

    private int findIndex(final String name) {
        Map<String, Integer> nameIndexMap = this.nameIndexMap;
        if (null == nameIndexMap) {
            nameIndexMap = this.nameIndexMap = createNameIndexMap(originMetadata);
        }
        final boolean caseSensitive = StringUtils.isQuoted(name);
        final String nameToSearch = caseSensitive ? StringUtils.unwrapQuotes(name) : name.toLowerCase(Locale.ROOT);

        return nameIndexMap.getOrDefault(nameToSearch, -1);
    }

    @Override
    public MySqlColumnDescriptor getColumnMetadata(String name) {
        requireNonNull(name, "name must not be null");
        final int idx = findIndex(name);
        if (idx < 0) {
            throw new NoSuchElementException("Column name '" + name + "' does not exist");
        }
        return originMetadata[idx];
    }

    @Override
    public boolean contains(String name) {
        requireNonNull(name, "name must not be null");

        return findIndex(name) >= 0;
    }

    @Override
    public List<MySqlColumnDescriptor> getColumnMetadatas() {
        return InternalArrays.asImmutableList(originMetadata);
    }

    @Override
    public String toString() {
        return "MySqlRowMetadata{metadata=" + Arrays.toString(originMetadata) + ", names=" +
            Arrays.toString(getNames(originMetadata)) + '}';
    }

    MySqlColumnDescriptor[] unwrap() {
        return originMetadata;
    }

    static MySqlRowMetadata create(DefinitionMetadataMessage[] columns) {
        int size = columns.length;
        MySqlColumnDescriptor[] metadata = new MySqlColumnDescriptor[size];

        for (int i = 0; i < size; ++i) {
            metadata[i] = MySqlColumnDescriptor.create(i, columns[i]);
        }

        return new MySqlRowMetadata(metadata);
    }

    private static String[] getNames(MySqlColumnDescriptor[] metadata) {
        int size = metadata.length;
        String[] names = new String[size];

        for (int i = 0; i < size; ++i) {
            names[i] = metadata[i].getName();
        }

        return names;
    }
}

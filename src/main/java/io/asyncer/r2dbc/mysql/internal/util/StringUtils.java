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

package io.asyncer.r2dbc.mysql.internal.util;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonEmpty;

/**
 * A utility for processing {@link String} in MySQL/MariaDB.
 */
public final class StringUtils {

    private static final char QUOTE = '`';

    /**
     * Quotes identifier with backticks, it will escape backticks in the identifier.
     *
     * @param identifier the identifier
     * @return quoted identifier
     */
    public static String quoteIdentifier(String identifier) {
        requireNonEmpty(identifier, "identifier must not be empty");

        int index = identifier.indexOf(QUOTE);

        if (index == -1) {
            return QUOTE + identifier + QUOTE;
        }

        int len = identifier.length();
        StringBuilder builder = new StringBuilder(len + 10).append(QUOTE);
        int fromIndex = 0;

        while (index != -1) {
            builder.append(identifier, fromIndex, index)
                .append(QUOTE)
                .append(QUOTE);
            fromIndex = index + 1;
            index = identifier.indexOf(QUOTE, fromIndex);
        }

        if (fromIndex < len) {
            builder.append(identifier, fromIndex, len);
        }

        return builder.append(QUOTE).toString();
    }

    /**
     * Extends a SQL statement with {@code RETURNING} clause.
     *
     * @param sql the original SQL statement.
     * @param returning quoted column identifiers.
     * @return the SQL statement with {@code RETURNING} clause.
     */
    public static String extendReturning(String sql, String returning) {
        return returning.isEmpty() ? sql : sql + " RETURNING " + returning;
    }

    private StringUtils() {
    }
}

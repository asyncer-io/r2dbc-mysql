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

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonEmpty;

/**
 * A utility for processing {@link String} and simple statements in MySQL/MariaDB.
 */
public final class StringUtils {

    private static final char QUOTE = '`';

    private static final String ZONE_PREFIX_POSIX = "posix/";

    private static final String ZONE_PREFIX_RIGHT = "right/";

    private static final int ZONE_PREFIX_LENGTH = 6;

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
     * @param sql       the original SQL statement.
     * @param returning quoted column identifiers.
     * @return the SQL statement with {@code RETURNING} clause.
     */
    public static String extendReturning(String sql, String returning) {
        return returning.isEmpty() ? sql : sql + " RETURNING " + returning;
    }

    /**
     * Generates a {@link String} indicating the statement timeout variable. e.g. {@code "max_statement_time=1.5"} for
     * MariaDB or {@code "max_execution_time=1500"} for MySQL.
     *
     * @param timeout   the statement timeout
     * @param isMariaDb whether the current server is MariaDB
     * @return the statement timeout variable
     */
    public static String statementTimeoutVariable(Duration timeout, boolean isMariaDb) {
        // mariadb: https://mariadb.com/kb/en/aborting-statements/
        // mysql: https://dev.mysql.com/blog-archive/server-side-select-statement-timeouts/
        // ref: https://github.com/mariadb-corporation/mariadb-connector-r2dbc
        if (isMariaDb) {
            // MariaDB supports fractional seconds with microsecond precision
            double seconds = (timeout.getSeconds() + timeout.getNano() / 1_000_000_000.0);
            return "max_statement_time=" + seconds;
        }

        return "max_execution_time=" + timeout.toMillis();
    }

    /**
     * Generates a statement to set the lock wait timeout for the current session. It is using InnoDB-specific session
     * variable {@code innodb_lock_wait_timeout}.
     *
     * @param timeout the lock wait timeout
     * @return the lock wait timeout statement
     */
    public static String lockWaitTimeoutStatement(Duration timeout) {
        return "SET innodb_lock_wait_timeout=" + timeout.getSeconds();
    }

    /**
     * Parses a normalized {@link ZoneId} from a time zone string of MySQL.
     * <p>
     * Note: since java 14.0.2, 11.0.8, 8u261 and 7u271, America/Nuuk is already renamed from America/Godthab. See also
     * <a href="https://mm.icann.org/pipermail/tz-announce/2020-April/000058.html">tzdata2020a</a>
     *
     * @param zoneId the time zone string
     * @return the normalized {@link ZoneId}
     * @throws IllegalArgumentException          if the time zone string is {@code null} or empty
     * @throws java.time.DateTimeException       if the time zone string has an invalid format
     * @throws java.time.zone.ZoneRulesException if the time zone string cannot be found
     */
    public static ZoneId parseZoneId(String zoneId) {
        requireNonEmpty(zoneId, "zoneId must not be empty");

        String realId;

        if (zoneId.startsWith(ZONE_PREFIX_POSIX) || zoneId.startsWith(ZONE_PREFIX_RIGHT)) {
            realId = zoneId.substring(ZONE_PREFIX_LENGTH);
        } else {
            realId = zoneId;
        }

        switch (realId) {
            case "Factory":
                // It seems like UTC.
                return ZoneOffset.UTC;
            case "ROC":
                // It is equal to +08:00.
                return ZoneOffset.ofHours(8);
        }

        return ZoneId.of(realId, ZoneId.SHORT_IDS).normalized();
    }

    private StringUtils() {
    }
}

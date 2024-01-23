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


import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Query logger to log queries.
 */
final class QueryLogger {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance("io.asyncer.r2dbc.mysql.QUERY");

    static void log(String query) {
        logger.debug("Executing statement [{}]", query);
    }

    static void log(int statementId, String query) {
        logger.debug("Prepared statement {} [{}]", statementId, query);
    }

    static void log(Query query, String returning, MySqlParameter[] values) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing statement [{}] with {}",
                StringUtils.extendReturning(query.getFormattedSql(), returning), values);
        }
    }

    static void log(int statementId, MySqlParameter[] values) {
        logger.debug("Executing prepared statement {} with {}", statementId, values);
    }

    static void logLocalInfile(String path) {
        logger.debug("Loading data from: {}", path);
    }

    private QueryLogger() { }
}

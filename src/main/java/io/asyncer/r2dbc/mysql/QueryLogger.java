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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query logger to log queries.
 */
final class QueryLogger {

    private static final Logger logger = LoggerFactory.getLogger("io.asyncer.r2dbc.mysql.QUERY");

    static void log(String query) {
        logger.debug("Executing direct query: {}", query);
    }

    static void log(Query query) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executing format query: {}", query.getFormattedSql());
        }
    }

    private QueryLogger() { }
}

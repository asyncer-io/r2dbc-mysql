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

package io.asyncer.r2dbc.mysql.cache;

import io.asyncer.r2dbc.mysql.Query;

/**
 * An abstraction that considers cache of query parsed results.
 */
public interface QueryCache {

    /**
     * Get {@link Query} if specified {@code key} has been cached, or cache the new {@link Query} parsed from
     * {@code key}.
     *
     * @param key the {@link Query} association key
     * @return the existing or parsed value associated with the {@code key}
     */
    Query get(String key);
}

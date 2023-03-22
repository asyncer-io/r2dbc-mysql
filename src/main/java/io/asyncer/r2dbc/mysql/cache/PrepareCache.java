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


import org.jetbrains.annotations.Nullable;

import java.util.function.IntConsumer;

/**
 * An abstraction that considers cache of statement prepared results.
 */
public interface PrepareCache {

    /**
     * Get the value of {@code key} in cache.
     *
     * @param key the key which want to get.
     * @return the value of {@code key}, which is usually prepared statement ID.
     */
    @Nullable
    Integer getIfPresent(String key);

    /**
     * Put the prepared result to the cache.
     *
     * @param key   the key of {@code value}, which is usually SQL statements.
     * @param value the value of {@code key}, which is usually prepared statement ID.
     * @param evict eviction value handler.
     * @return {@code true} if {@code value} has been put succeed.
     */
    boolean putIfAbsent(String key, int value, IntConsumer evict);
}

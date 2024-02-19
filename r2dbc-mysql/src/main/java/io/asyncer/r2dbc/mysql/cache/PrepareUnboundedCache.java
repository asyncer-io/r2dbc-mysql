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

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntConsumer;

/**
 * An unbounded implementation of {@link PrepareCache}.
 */
final class PrepareUnboundedCache extends ConcurrentHashMap<String, Integer> implements PrepareCache {

    @Override
    public Integer getIfPresent(String key) {
        return super.get(key);
    }

    @Override
    public boolean putIfAbsent(String key, int value, IntConsumer evict) {
        return super.putIfAbsent(key, value) == null;
    }
}

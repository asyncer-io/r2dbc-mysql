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

package io.asyncer.r2dbc.mysql.api;

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlTransactionDefinition} for immutable transaction definition.
 *
 * @since 1.1.3
 */
final class SimpleTransactionDefinition implements MySqlTransactionDefinition {

    static final SimpleTransactionDefinition EMPTY = new SimpleTransactionDefinition(Collections.emptyMap());

    private final Map<Option<?>, Object> options;

    private SimpleTransactionDefinition(Map<Option<?>, Object> options) {
        this.options = options;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getAttribute(Option<T> option) {
        return (T) this.options.get(option);
    }

    @Override
    public MySqlTransactionDefinition isolationLevel(IsolationLevel isolationLevel) {
        requireNonNull(isolationLevel, "isolationLevel must not be null");

        return with(ISOLATION_LEVEL, isolationLevel);
    }

    @Override
    public MySqlTransactionDefinition withoutIsolationLevel() {
        return without(ISOLATION_LEVEL);
    }

    @Override
    public MySqlTransactionDefinition readOnly() {
        return with(READ_ONLY, true);
    }

    @Override
    public MySqlTransactionDefinition readWrite() {
        return with(READ_ONLY, false);
    }

    @Override
    public MySqlTransactionDefinition withoutMutability() {
        return without(READ_ONLY);
    }

    @Override
    public MySqlTransactionDefinition lockWaitTimeout(Duration timeout) {
        requireNonNull(timeout, "timeout must not be null");

        return with(LOCK_WAIT_TIMEOUT, timeout);
    }

    @Override
    public MySqlTransactionDefinition withoutLockWaitTimeout() {
        return without(LOCK_WAIT_TIMEOUT);
    }

    @Override
    public MySqlTransactionDefinition consistent() {
        return with(WITH_CONSISTENT_SNAPSHOT, true);
    }

    @Override
    public MySqlTransactionDefinition consistent(String engine) {
        requireNonNull(engine, "engine must not be null");

        return consistent0(CONSISTENT_SNAPSHOT_ENGINE, engine);
    }

    @Override
    public MySqlTransactionDefinition consistent(String engine, long sessionId) {
        requireNonNull(engine, "engine must not be null");

        Map<Option<?>, Object> options = new HashMap<>(this.options);

        options.put(WITH_CONSISTENT_SNAPSHOT, true);
        options.put(CONSISTENT_SNAPSHOT_ENGINE, engine);
        options.put(CONSISTENT_SNAPSHOT_FROM_SESSION, sessionId);

        return of(options);
    }

    @Override
    public MySqlTransactionDefinition consistentFromSession(long sessionId) {
        return consistent0(CONSISTENT_SNAPSHOT_FROM_SESSION, sessionId);
    }

    @Override
    public MySqlTransactionDefinition withoutConsistent() {
        if (this.options.isEmpty()) {
            return this;
        }

        Map<Option<?>, Object> options = new HashMap<>(this.options);

        options.remove(WITH_CONSISTENT_SNAPSHOT);
        options.remove(CONSISTENT_SNAPSHOT_ENGINE);
        options.remove(CONSISTENT_SNAPSHOT_FROM_SESSION);

        return of(options);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SimpleTransactionDefinition)) {
            return false;
        }

        SimpleTransactionDefinition that = (SimpleTransactionDefinition) o;

        return options.equals(that.options);
    }

    @Override
    public int hashCode() {
        return options.hashCode();
    }

    @Override
    public String toString() {
        return "SimpleTransactionDefinition" + options;
    }

    private <T> MySqlTransactionDefinition with(Option<T> option, T value) {
        if (this.options.isEmpty()) {
            return new SimpleTransactionDefinition(Collections.singletonMap(option, value));
        }

        if (value.equals(this.options.get(option))) {
            return this;
        }

        Map<Option<?>, Object> options = new HashMap<>(this.options);

        options.put(option, value);

        return of(options);
    }

    private SimpleTransactionDefinition without(Option<?> option) {
        requireNonNull(option, "option must not be null");

        if (!this.options.containsKey(option)) {
            return this;
        }

        if (this.options.size() == 1) {
            return EMPTY;
        }

        Map<Option<?>, Object> options = new HashMap<>(this.options);

        options.remove(option);

        return of(options);
    }

    private <T> MySqlTransactionDefinition consistent0(Option<T> option, T value) {
        if (Boolean.TRUE.equals(this.options.get(WITH_CONSISTENT_SNAPSHOT))) {
            return with(option, value);
        }

        Map<Option<?>, Object> options = new HashMap<>(this.options);

        options.put(WITH_CONSISTENT_SNAPSHOT, true);
        options.put(option, value);

        return of(options);
    }

    private static SimpleTransactionDefinition of(Map<Option<?>, Object> options) {
        switch (options.size()) {
            case 0:
                return EMPTY;
            case 1: {
                Map.Entry<Option<?>, Object> e = options.entrySet().iterator().next();

                return new SimpleTransactionDefinition(Collections.singletonMap(e.getKey(), e.getValue()));
            }
            default:
                return new SimpleTransactionDefinition(options);
        }
    }
}

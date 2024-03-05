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

import io.r2dbc.spi.IsolationLevel;

/**
 * An internal interface for check, set and reset connection states.
 */
interface ConnectionState {

    /**
     * Sets current isolation level.
     *
     * @param level current level.
     */
    void setIsolationLevel(IsolationLevel level);

    /**
     * Returns session lock wait timeout.
     *
     * @return Session lock wait timeout.
     */
    long getSessionLockWaitTimeout();

    /**
     * Sets current lock wait timeout.
     *
     * @param timeoutSeconds seconds of current lock wait timeout.
     */
    void setCurrentLockWaitTimeout(long timeoutSeconds);

    /**
     * Checks if lock wait timeout has been changed by {@link #setCurrentLockWaitTimeout(long)}.
     *
     * @return if lock wait timeout changed.
     */
    boolean isLockWaitTimeoutChanged();

    /**
     * Resets current isolation level in initial state.
     */
    void resetIsolationLevel();

    /**
     * Resets current isolation level in initial state.
     */
    void resetCurrentLockWaitTimeout();

    /**
     * Checks if connection is processing a transaction.
     *
     * @return if in a transaction.
     */
    boolean isInTransaction();
}

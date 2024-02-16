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

package io.asyncer.r2dbc.mysql.message;

import io.netty.util.ReferenceCounted;

/**
 * A sealed interface for field, it has 3-implementations: {@link NullFieldValue}, {@link NormalFieldValue}
 * and {@link LargeFieldValue}.
 * <p>
 * WARNING: it is sealed interface, should NEVER extend or implemented by another interface or class.
 */
public interface FieldValue extends ReferenceCounted {

    /**
     * Checks if value is {@code null}.
     *
     * @return if value is {@code null}.
     */
    default boolean isNull() {
        return false;
    }

    /**
     * Gets an instance for {@code null} value.
     *
     * @return a field contains a {@code null} value.
     */
    static FieldValue nullField() {
        return NullFieldValue.INSTANCE;
    }
}

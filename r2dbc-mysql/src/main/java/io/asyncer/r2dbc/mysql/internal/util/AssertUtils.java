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

package io.asyncer.r2dbc.mysql.internal.util;

import org.jetbrains.annotations.Nullable;

/**
 * Assertion library for {@literal r2dbc-mysql} implementation.
 */
public final class AssertUtils {

    /**
     * Checks that an object is not {@code null} and throws a customized {@link IllegalArgumentException} if
     * it is.
     *
     * @param obj     the object reference to check for nullity.
     * @param message the detail message to be used by thrown {@link IllegalArgumentException}.
     * @param <T>     the type of the reference.
     * @return {@code obj} if not {@code null}.
     * @throws IllegalArgumentException if {@code obj} is {@code null}.
     */
    public static <T> T requireNonNull(@Nullable T obj, String message) {
        if (obj == null) {
            throw new IllegalArgumentException(message);
        }

        return obj;
    }

    /**
     * Checks that a condition is accepted and throws a customized {@link IllegalArgumentException} if it is
     * not.
     *
     * @param condition if condition accepted.
     * @param message   the detail message to be used by thrown {@link IllegalArgumentException}.
     * @throws IllegalArgumentException if {@code condition} is {@code false}.
     */
    public static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Checks that a {@link String} is neither {@code null} nor empty, and throws a customized
     * {@link IllegalArgumentException} if it is.
     *
     * @param s       the string to check for empty.
     * @param message the detail message to be used by thrown {@link IllegalArgumentException}.
     * @throws IllegalArgumentException if {@code s} is {@code null} or empty.
     */
    public static void requireNonEmpty(@Nullable String s, String message) {
        if (s == null || s.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }

    private AssertUtils() { }
}

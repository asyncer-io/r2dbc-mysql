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

package io.asyncer.r2dbc.mysql.codec;

import io.asyncer.r2dbc.mysql.api.MySqlReadableMetadata;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;

/**
 * Codec for primitive types, like {@code int} or {@code double}.
 *
 * @param <T> the boxed type of handling primitive data.
 */
abstract class AbstractPrimitiveCodec<T> implements PrimitiveCodec<T> {

    private final Class<T> primitiveClass;

    private final Class<T> boxedClass;

    AbstractPrimitiveCodec(Class<T> primitiveClass, Class<T> boxedClass) {
        require(primitiveClass.isPrimitive() && !boxedClass.isPrimitive(),
            "primitiveClass must be primitive and boxedClass must not be primitive");

        this.primitiveClass = primitiveClass;
        this.boxedClass = boxedClass;
    }

    @Override
    public final boolean canDecode(MySqlReadableMetadata metadata, Class<?> target) {
        return (target.isAssignableFrom(boxedClass) || target.equals(primitiveClass)) && doCanDecode(metadata);
    }

    @Override
    public final Class<T> getPrimitiveClass() {
        return primitiveClass;
    }

    @Override
    public final Class<? extends T> getMainClass() {
        return boxedClass;
    }

    protected abstract boolean doCanDecode(MySqlReadableMetadata metadata);
}

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

import io.asyncer.r2dbc.mysql.MySqlColumnMetadata;
import io.netty.buffer.ByteBuf;

/**
 * Base class considers primitive class for {@link Codec} implementations. It should be an internal
 * abstraction.
 * <p>
 * Primitive types should never return {@code null} when decoding.
 *
 * @param <T> the boxed type that is handled by this codec.
 */
interface PrimitiveCodec<T> extends Codec<T> {

    /**
     * Decodes a {@link ByteBuf} as specified {@link Class}.
     *
     * @param value    the {@link ByteBuf}.
     * @param metadata the metadata of the column.
     * @param target   the specified {@link Class}, which can be a primitive type.
     * @param binary   if the value should be decoded by binary protocol.
     * @param context  the codec context.
     * @return the decoded data that is boxed.
     */
    @Override
    T decode(ByteBuf value, MySqlColumnMetadata metadata, Class<?> target, boolean binary,
        CodecContext context);

    /**
     * Checks if the field value can be decoded as a primitive data.
     *
     * @param metadata the metadata of the column.
     * @return if it can decode.
     */
    boolean canPrimitiveDecode(MySqlColumnMetadata metadata);

    /**
     * Gets the primitive {@link Class}, such as {@link Integer#TYPE}, etc.
     *
     * @return the primitive {@link Class}.
     */
    Class<T> getPrimitiveClass();
}

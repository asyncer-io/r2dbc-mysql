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

import io.asyncer.r2dbc.mysql.MySqlParameter;
import io.asyncer.r2dbc.mysql.api.MySqlReadableMetadata;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

/**
 * Codec to encode and decode values based on MySQL data binary/text protocol.
 * <p>
 * Use {@link ParameterizedCodec} for support {@code ParameterizedType} encoding/decoding.
 *
 * @param <T> the type that is handled by this codec.
 */
public interface Codec<T> {

    /**
     * Decodes a {@link ByteBuf} as specified {@link Class}.
     *
     * @param value    the {@link ByteBuf}.
     * @param metadata the metadata of the column or the {@code OUT} parameter.
     * @param target   the specified {@link Class}.
     * @param binary   if the value should be decoded by binary protocol.
     * @param context  the codec context.
     * @return the decoded result.
     */
    @Nullable
    T decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context);

    /**
     * Checks if the field value can be decoded as specified {@link Class}.
     *
     * @param metadata the metadata of the column or the {@code OUT} parameter.
     * @param target   the specified {@link Class}.
     * @return if it can decode.
     */
    boolean canDecode(MySqlReadableMetadata metadata, Class<?> target);

    /**
     * Checks if it can encode the specified value.
     *
     * @param value the specified value.
     * @return if it can encode.
     */
    boolean canEncode(Object value);

    /**
     * Encode a value to a {@link MySqlParameter}.
     *
     * @param value   the specified value.
     * @param context the codec context.
     * @return encoded {@link MySqlParameter}.
     */
    MySqlParameter encode(Object value, CodecContext context);
}

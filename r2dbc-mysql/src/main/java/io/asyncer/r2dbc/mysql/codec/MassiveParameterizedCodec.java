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
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.util.List;

/**
 * An interface considers massive data with {@link ParameterizedType} for {@link Codec}.
 *
 * @param <T> the type that is handled by this codec.
 */
public interface MassiveParameterizedCodec<T> extends ParameterizedCodec<T>, MassiveCodec<T> {

    /**
     * Decode a massive value as specified {@link ParameterizedType}.
     *
     * @param value    {@link ByteBuf}s list.
     * @param metadata the metadata of the column or the {@code OUT} parameter.
     * @param target   the specified {@link ParameterizedType}.
     * @param binary   if the value should be decoded by binary protocol.
     * @param context  the codec context.
     * @return the decoded result.
     */
    @Nullable
    Object decodeMassive(List<ByteBuf> value, MySqlReadableMetadata metadata, ParameterizedType target,
        boolean binary, CodecContext context);
}

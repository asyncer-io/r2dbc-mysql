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

package io.asyncer.r2dbc.mysql.client;

import io.netty.buffer.ByteBuf;
import reactor.core.Disposable;

/**
 * An abstraction considers to compress and decompress data.
 */
interface Compressor extends Disposable {

    /**
     * Compresses the given {@link ByteBuf}. It does not guarantee that the compressed data is smaller than
     * the original. It will not change the reader index of the given {@link ByteBuf}. It may return early if
     * the compressed data is not smaller than the original.
     *
     * @param buf the {@link ByteBuf} to compress
     * @return the compressed {@link ByteBuf}
     */
    ByteBuf compress(ByteBuf buf);

    /**
     * Decompresses the given {@link ByteBuf}.
     *
     * @param buf              the {@link ByteBuf} to decompress
     * @param uncompressedSize the size of the uncompressed data
     * @return the decompressed {@link ByteBuf}
     */
    ByteBuf decompress(ByteBuf buf, int uncompressedSize);
}

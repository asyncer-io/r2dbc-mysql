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

import com.github.luben.zstd.Zstd;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;

/**
 * An implementation of {@link Compressor} that uses the Z-standard compression algorithm.
 *
 * @see <a href="https://facebook.github.io/zstd/">Zstandard</a>
 */
final class ZstdCompressor implements Compressor {

    private final int compressionLevel;

    ZstdCompressor(int compressionLevel) {
        require(
            compressionLevel >= Zstd.minCompressionLevel() && compressionLevel <= Zstd.maxCompressionLevel(),
            "compressionLevel must be a value of Z standard compression levels");

        this.compressionLevel = compressionLevel;
    }

    @Override
    public ByteBuf compress(ByteBuf buf) {
        ByteBuffer buffer = Zstd.compress(buf.nioBuffer(), compressionLevel);
        return Unpooled.wrappedBuffer(buffer);
    }

    @Override
    public ByteBuf decompress(ByteBuf buf, int uncompressedSize) {
        ByteBuffer buffer = Zstd.decompress(buf.nioBuffer(), uncompressedSize);
        return Unpooled.wrappedBuffer(buffer);
    }

    @Override
    public void dispose() {
        // Do nothing
    }
}

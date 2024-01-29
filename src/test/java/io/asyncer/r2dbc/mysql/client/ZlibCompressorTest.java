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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;
import java.util.zip.DeflaterOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Unit tests for {@link ZlibCompressor}.
 */
class ZlibCompressorTest {

    private final ZlibCompressor compressor = new ZlibCompressor();

    @ParameterizedTest
    @MethodSource("uncompressedData")
    void compress(String input) throws IOException {
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        ByteBuf compressed = compressor.compress(Unpooled.wrappedBuffer(bytes));
        byte[] nativeCompressed = nativeCompress(bytes);

        // It may return early if the compressed data is not smaller than the original.
        assertThat(ByteBufUtil.getBytes(compressed)).hasSizeLessThanOrEqualTo(bytes.length)
            .isEqualTo(Arrays.copyOf(nativeCompressed, compressed.readableBytes()));
    }

    @ParameterizedTest
    @MethodSource("uncompressedData")
    void decompress(String input) throws IOException {
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        ByteBuf compressed = Unpooled.wrappedBuffer(nativeCompress(bytes));
        ByteBuf decompressed = compressor.decompress(compressed, bytes.length);

        assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(bytes);
    }

    @Test
    void badDecompress() {
        ByteBuf compressed = Unpooled.wrappedBuffer(
            new byte[] { 0x78, 0x7c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 });

        assertThatExceptionOfType(DecoderException.class)
            .isThrownBy(() -> compressor.decompress(compressed, compressed.readableBytes() << 1));
    }

    static Stream<String> uncompressedData() {
        return Stream.of(
            "", " ",
            "Hello, world!",
            "1234567890",
            "ユニコードテスト、유니코드 테스트,Unicode测试，тест Юникода",
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, " +
                "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. " +
                "Ut enim ad minim veniam, quis exercitation ullamco nisi ut aliquip ea commodo consequat. " +
                "Duis aute irure dolor in reprehenderit en voluptate esse cillum eu fugiat nulla pariatur."
        );
    }

    private static byte[] nativeCompress(byte[] input) throws IOException {
        try (ByteArrayOutputStream r = new ByteArrayOutputStream();
            DeflaterOutputStream s = new DeflaterOutputStream(r)) {

            s.write(input);
            s.finish();
            s.flush();

            return r.toByteArray();
        }
    }
}

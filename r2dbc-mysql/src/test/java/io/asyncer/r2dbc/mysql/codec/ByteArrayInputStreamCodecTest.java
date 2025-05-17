/*
 * Copyright 2025 asyncer.io projects
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testcontainers.shaded.org.bouncycastle.util.encoders.Hex;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Unit tests for {@link ByteArrayInputStreamCodec}.
 */
public class ByteArrayInputStreamCodecTest implements CodecTestSupport<ByteArrayInputStream> {

    private final byte[][] rawData = {
            new byte[0],
            new byte[] { 0x7F },
            new byte[] { 0x12, 34, 0x56, 78, (byte) 0x9A },
            "Hello world!".getBytes(StandardCharsets.US_ASCII),
            new byte[] { (byte) 0xFE, (byte) 0xDC, (byte) 0xBA },
    };

    private final ByteArrayInputStream[] data = Arrays.stream(rawData)
            .map(ByteArrayInputStream::new)
            .toArray(ByteArrayInputStream[]::new);

    @Override
    public Codec<ByteArrayInputStream> getCodec() {
        return ByteArrayInputStreamCodec.INSTANCE;
    }

    @Override
    public ByteArrayInputStream[] originParameters() {
        return data;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(rawData)
                .map(bytes -> String.format("x'%s'", Hex.toHexString(bytes)))
                .toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(rawData)
                .map(Unpooled::wrappedBuffer)
                .toArray(ByteBuf[]::new);
    }
}

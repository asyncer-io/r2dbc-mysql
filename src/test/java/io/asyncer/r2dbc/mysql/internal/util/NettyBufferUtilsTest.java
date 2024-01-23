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

package io.asyncer.r2dbc.mysql.internal.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Unit tests for {@link NettyBufferUtils}.
 */
class NettyBufferUtilsTest {

    @ParameterizedTest
    @ValueSource(strings = { "stations.csv", "users.csv" })
    void readFile(String name) throws IOException, URISyntaxException {
        URL url = Objects.requireNonNull(getClass().getResource("/local/" + name));
        Path path = Paths.get(Paths.get(url.toURI()).toString());
        String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);

        NettyBufferUtils.readFile(path, PooledByteBufAllocator.DEFAULT, 8192)
            .map(NettyBufferUtilsTest::toStringAndRelease)
            .as(it -> StepVerifier.create(it, Long.MAX_VALUE))
            .expectNext(content)
            .verifyComplete();
    }

    private static String toStringAndRelease(ByteBuf buf) {
        String s = buf.toString(StandardCharsets.UTF_8);
        buf.release();
        return s;
    }
}

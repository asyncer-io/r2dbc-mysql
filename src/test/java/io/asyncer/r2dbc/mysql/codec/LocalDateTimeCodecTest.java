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

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.Arrays;

import static io.asyncer.r2dbc.mysql.codec.LocalDateCodecTest.DATES;
import static io.asyncer.r2dbc.mysql.codec.LocalTimeCodecTest.TIMES;

/**
 * Unit tests for {@link LocalDateTimeCodec}.
 */
class LocalDateTimeCodecTest extends DateTimeCodecTestSupport<LocalDateTime> {

    private final LocalDateTime[] dateTimes = multiple();

    @Override
    public LocalDateTimeCodec getCodec() {
        return LocalDateTimeCodec.INSTANCE;
    }

    @Override
    public LocalDateTime[] originParameters() {
        return dateTimes;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(dateTimes).map(this::toText).toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(dateTimes).map(this::toBinary).toArray(ByteBuf[]::new);
    }

    private static LocalDateTime[] multiple() {
        LocalDateTime[] results = new LocalDateTime[DATES.length * TIMES.length];
        for (int i = 0; i < DATES.length; ++i) {
            for (int j = 0; j < TIMES.length; ++j) {
                results[i * (TIMES.length) + j] = LocalDateTime.of(DATES[i], TIMES[j]);
            }
        }
        return results;
    }
}

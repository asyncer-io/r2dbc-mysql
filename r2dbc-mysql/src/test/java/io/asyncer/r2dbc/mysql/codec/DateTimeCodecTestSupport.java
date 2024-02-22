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

import io.asyncer.r2dbc.mysql.ConnectionContextTest;
import io.netty.buffer.ByteBuf;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.Temporal;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * Base class considers codecs unit tests of date/time.
 */
abstract class DateTimeCodecTestSupport<T extends Temporal> implements CodecTestSupport<T> {

    protected static final ZoneOffset ENCODE_SERVER_ZONE = ZoneOffset.ofHours(6);

    private static final DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder()
        .appendLiteral('\'')
        .appendValue(YEAR, 4, 19, SignStyle.NORMAL)
        .appendLiteral('-')
        .appendValue(MONTH_OF_YEAR, 2)
        .appendLiteral('-')
        .appendValue(DAY_OF_MONTH, 2)
        .appendLiteral(' ')
        .appendValue(HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2)
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2)
        .optionalStart()
        .appendFraction(MICRO_OF_SECOND, 0, 6, true)
        .optionalEnd()
        .appendLiteral('\'')
        .toFormatter(Locale.ENGLISH);

    static {
        System.setProperty("user.timezone", "GMT+2");
    }

    @Override
    public CodecContext context() {
        return ConnectionContextTest.mock(false, ENCODE_SERVER_ZONE);
    }

    protected final String toText(Temporal dateTime) {
        return FORMATTER.format(dateTime);
    }

    protected final ByteBuf toBinary(LocalDateTime dateTime) {
        ByteBuf buf = LocalDateCodecTest.encode(dateTime.toLocalDate());
        LocalTime time = dateTime.toLocalTime();

        if (LocalTime.MIDNIGHT.equals(time)) {
            return buf;
        }

        buf.writeByte(time.getHour())
            .writeByte(time.getMinute())
            .writeByte(time.getSecond());

        if (time.getNano() != 0) {
            buf.writeIntLE((int) TimeUnit.NANOSECONDS.toMicros(time.getNano()));
        }

        return buf;
    }
}

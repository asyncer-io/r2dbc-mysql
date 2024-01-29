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

package io.asyncer.r2dbc.mysql.constant;

/**
 * Constants for MySQL protocol packets.
 * <p>
 * WARNING: do NOT use it outer than {@literal r2dbc-mysql}.
 */
public final class Packets {

    /**
     * The length of the byte size field, it is 3 bytes.
     */
    public static final int SIZE_FIELD_SIZE = 3;

    /**
     * The max bytes size of payload, value is 16777215. (i.e. max value of int24, (2 ** 24) - 1)
     */
    public static final int MAX_PAYLOAD_SIZE = 0xFFFFFF;

    /**
     * The header size of a compression frame, which includes entire frame size (unsigned int24), compression
     * sequence id (unsigned int8) and compressed size (unsigned int24).
     */
    public static final int COMPRESS_HEADER_SIZE = SIZE_FIELD_SIZE + 1 + SIZE_FIELD_SIZE;

    /**
     * The header size of a normal frame, which includes entire frame size (unsigned int24) and normal
     * sequence id (unsigned int8).
     */
    public static final int NORMAL_HEADER_SIZE = SIZE_FIELD_SIZE + 1;

    /**
     * The terminal of C-style string or C-style binary data.
     */
    public static final byte TERMINAL = 0;

    private Packets() { }
}

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

package io.asyncer.r2dbc.mysql.constant;

/**
 * The compression algorithm for client/server communication.
 */
public enum CompressionAlgorithm {

    /**
     * Do not use compression protocol.
     */
    UNCOMPRESSED,

    /**
     * Use zlib compression algorithm for client/server communication.
     * <p>
     * If zlib is not available, the connection will throw an exception when logging in.
     */
    ZLIB,

    /**
     * Use Z-Standard compression algorithm for client/server communication.
     * <p>
     * If zstd is not available, the connection will throw an exception when logging in.
     */
    ZSTD,
}

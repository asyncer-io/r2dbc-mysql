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

package io.asyncer.r2dbc.mysql;

import io.asyncer.r2dbc.mysql.constant.CompressionAlgorithm;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * Integration tests for zstd compression.
 */
@EnabledIf("envIsZstdSupported")
class ZstdCompressionIntegrationTest extends CompressionIntegrationTestSupport {

    ZstdCompressionIntegrationTest() {
        super(CompressionAlgorithm.ZSTD);
    }

    static boolean envIsZstdSupported() {
        String type = System.getProperty("test.db.type");

        if ("mariadb".equalsIgnoreCase(type)) {
            return false;
        }

        String version = System.getProperty("test.mysql.version");

        if (version == null || version.isEmpty()) {
            return true;
        }

        ServerVersion ver = ServerVersion.parse(version);
        return ver.isGreaterThanOrEqualTo(ServerVersion.create(8, 0, 18));
    }
}

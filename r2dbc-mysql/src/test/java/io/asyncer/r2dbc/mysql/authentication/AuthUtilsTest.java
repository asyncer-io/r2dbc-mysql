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

package io.asyncer.r2dbc.mysql.authentication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * Unit tests for {@link AuthUtils}
 */
public class AuthUtilsTest {

    static byte[] seedBytes;

    static {
        try {
            seedBytes = generateSalt(20);
        } catch (NoSuchAlgorithmException e) {
            seedBytes = "random".getBytes();
        }
    }
    
    @Test
    void rotatingXor() {
        byte[] password = "abc123".getBytes();

        assertDoesNotThrow(() -> AuthUtils.rotatingXor(password, seedBytes));
    }

    static byte[] generateSalt(int length) throws NoSuchAlgorithmException {
        SecureRandom sr = SecureRandom.getInstance("SHA1PRNG");
        byte[] salt = new byte[length];
        sr.nextBytes(salt);
        return salt;
    }
}

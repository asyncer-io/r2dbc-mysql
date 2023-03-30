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

package io.asyncer.r2dbc.mysql.authentication;

import io.asyncer.r2dbc.mysql.collation.CharCollation;
import org.jetbrains.annotations.Nullable;

import java.nio.CharBuffer;

import static io.asyncer.r2dbc.mysql.constant.Envelopes.TERMINAL;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlAuthProvider} for type "mysql_clear_password".
 */
public final class MySqlClearAuthProvider implements MySqlAuthProvider {

    static MySqlClearAuthProvider getInstance() {
        return LazyHolder.INSTANCE;
    }

    @Override
    public boolean isSslNecessary() {
        return true;
    }

    @Override
    public byte[] authentication(@Nullable CharSequence password, byte[] salt, CharCollation collation) {
        if (password == null || password.length() <= 0) {
            return new byte[] { TERMINAL };
        }

        requireNonNull(collation, "collation must not be null when password exists");

        return AuthUtils.encodeTerminal(CharBuffer.wrap(password), collation.getCharset());
    }

    @Override
    public MySqlAuthProvider next() {
        return this;
    }

    @Override
    public String getType() {
        return MYSQL_CLEAR_PASSWORD;
    }

    private MySqlClearAuthProvider() {
    }

    private static class LazyHolder {
        private static final MySqlClearAuthProvider INSTANCE = new MySqlClearAuthProvider();
    }
}

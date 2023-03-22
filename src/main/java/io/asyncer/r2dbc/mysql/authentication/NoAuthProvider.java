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

import static io.asyncer.r2dbc.mysql.util.InternalArrays.EMPTY_BYTES;

/**
 * An implementation of {@link MySqlAuthProvider} when server does not set {@code Capability.PLUGIN_AUTH}. And
 * {@code ChangeAuthMessage} will be sent by server after handshake response.
 */
final class NoAuthProvider implements MySqlAuthProvider {

    static final NoAuthProvider INSTANCE = new NoAuthProvider();

    @Override
    public boolean isSslNecessary() {
        return false;
    }

    @Override
    public byte[] authentication(@Nullable CharSequence password, byte[] salt, CharCollation collation) {
        // Has no authentication provider in here.
        return EMPTY_BYTES;
    }

    @Override
    public MySqlAuthProvider next() {
        return this;
    }

    @Override
    public String getType() {
        return NO_AUTH_PROVIDER;
    }

    private NoAuthProvider() { }
}

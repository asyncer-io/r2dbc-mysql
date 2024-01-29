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

package io.asyncer.r2dbc.mysql.message.server;

import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.netty.buffer.ByteBuf;

/**
 * A message sent by the server to indicate that the client should send a file to the server using the
 * {@code LOAD DATA LOCAL INFILE} command.
 */
public final class LocalInfileRequest implements ServerMessage {

    private final String path;

    private LocalInfileRequest(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    static LocalInfileRequest decode(ByteBuf buf, ConnectionContext context) {
        buf.skipBytes(1); // Constant 0xFB
        return new LocalInfileRequest(buf.toString(context.getClientCollation().getCharset()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LocalInfileRequest)) {
            return false;
        }

        LocalInfileRequest that = (LocalInfileRequest) o;

        return path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public String toString() {
        return "LocalInfileRequest{path='" + path + "'}";
    }
}

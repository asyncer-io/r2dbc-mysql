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

package io.asyncer.r2dbc.mysql.message.client;

import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.netty.buffer.ByteBuf;

public final class InitDbMessage extends ScalarClientMessage {

    private static final byte FLAG = 0x02;

    private final String database;

    public InitDbMessage(String database) {
        this.database = database;
    }

    @Override
    protected void writeTo(ByteBuf buf, ConnectionContext context) {
        // RestOfPacketString, no need terminal or length
        buf.writeByte(FLAG).writeCharSequence(database, context.getClientCollation().getCharset());
    }
}

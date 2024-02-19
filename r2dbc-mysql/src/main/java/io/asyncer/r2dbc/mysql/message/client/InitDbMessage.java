package io.asyncer.r2dbc.mysql.message.client;

import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.netty.buffer.ByteBuf;

public final class InitDbMessage extends ScalarClientMessage {

    private static final byte FLAG = 0x02;

    private final String database;

    public InitDbMessage(String database) { this.database = database; }

    @Override
    protected void writeTo(ByteBuf buf, ConnectionContext context) {
        // RestOfPacketString, no need terminal or length
        buf.writeByte(FLAG).writeCharSequence(database, context.getClientCollation().getCharset());
    }
}

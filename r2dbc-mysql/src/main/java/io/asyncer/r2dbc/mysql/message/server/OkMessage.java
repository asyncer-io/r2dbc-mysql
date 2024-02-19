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

package io.asyncer.r2dbc.mysql.message.server;

import io.asyncer.r2dbc.mysql.Capability;
import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.asyncer.r2dbc.mysql.constant.ServerStatuses;
import io.asyncer.r2dbc.mysql.internal.util.VarIntUtils;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * OK message, it may be a complete signal of command, or a succeed signal for the Connection Phase of
 * connection lifecycle.
 * <p>
 * Note: OK message are also used to indicate EOF and EOF message are deprecated as of MySQL 5.7.5.
 */
public final class OkMessage implements WarningMessage, ServerStatusMessage, CompleteMessage {

    private static final int SESSION_TRACK_SYSTEM_VARIABLES = 0;

    private static final int MIN_SIZE = 7;

    private final boolean isEndOfRows;

    private final long affectedRows;

    /**
     * Last insert-id, not business id on table.
     */
    private final long lastInsertId;

    private final short serverStatuses;

    private final int warnings;

    private final String information;

    private final Map<String, String> systemVariables;

    private OkMessage(boolean isEndOfRows, long affectedRows, long lastInsertId, short serverStatuses,
        int warnings, String information, Map<String, String> systemVariables) {
        this.isEndOfRows = isEndOfRows;
        this.affectedRows = affectedRows;
        this.lastInsertId = lastInsertId;
        this.serverStatuses = serverStatuses;
        this.warnings = warnings;
        this.information = requireNonNull(information, "information must not be null");
        this.systemVariables = requireNonNull(systemVariables, "systemVariables must not be null");
    }

    public boolean isEndOfRows() {
        return isEndOfRows;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public long getLastInsertId() {
        return lastInsertId;
    }

    @Override
    public short getServerStatuses() {
        return serverStatuses;
    }

    @Override
    public int getWarnings() {
        return warnings;
    }

    @Nullable
    public String getSystemVariable(String key) {
        return systemVariables.get(key);
    }

    @Override
    public boolean isDone() {
        return (serverStatuses & ServerStatuses.MORE_RESULTS_EXISTS) == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OkMessage)) {
            return false;
        }

        OkMessage okMessage = (OkMessage) o;

        return isEndOfRows == okMessage.isEndOfRows &&
            affectedRows == okMessage.affectedRows &&
            lastInsertId == okMessage.lastInsertId &&
            serverStatuses == okMessage.serverStatuses &&
            warnings == okMessage.warnings &&
            information.equals(okMessage.information) &&
            systemVariables.equals(okMessage.systemVariables);
    }

    @Override
    public int hashCode() {
        int result = (isEndOfRows ? 1 : 0);
        result = 31 * result + (int) (affectedRows ^ (affectedRows >>> 32));
        result = 31 * result + (int) (lastInsertId ^ (lastInsertId >>> 32));
        result = 31 * result + serverStatuses;
        result = 31 * result + warnings;
        result = 31 * result + information.hashCode();
        return 31 * result + systemVariables.hashCode();
    }

    @Override
    public String toString() {
        if (warnings == 0) {
            return "OkMessage{isEndOfRows=" + isEndOfRows +
                ", affectedRows=" + Long.toUnsignedString(affectedRows) +
                ", lastInsertId=" + Long.toUnsignedString(lastInsertId) +
                ", serverStatuses=" + Integer.toHexString(serverStatuses) +
                ", information='" + information +
                "', systemVariables=" + systemVariables +
                '}';
        }

        return "OkMessage{isEndOfRows=" + isEndOfRows +
            ", affectedRows=" + Long.toUnsignedString(affectedRows) +
            ", lastInsertId=" + Long.toUnsignedString(lastInsertId) +
            ", serverStatuses=" + Integer.toHexString(serverStatuses) +
            ", warnings=" + warnings +
            ", information='" + information +
            "', systemVariables=" + systemVariables +
            "}";
    }

    static boolean isValidSize(int bytes) {
        return bytes >= MIN_SIZE;
    }

    static OkMessage decode(boolean isEndOfRows, ByteBuf buf, ConnectionContext context) {
        buf.skipBytes(1); // OK message header, 0x00 or 0xFE

        Capability capability = context.getCapability();
        long affectedRows = VarIntUtils.readVarInt(buf);
        long lastInsertId = VarIntUtils.readVarInt(buf);
        short serverStatuses;
        int warnings;

        if (capability.isProtocol41()) {
            serverStatuses = buf.readShortLE();
            warnings = buf.readUnsignedShortLE();
        } else if (capability.isTransactionAllowed()) {
            serverStatuses = buf.readShortLE();
            warnings = 0;
        } else {
            warnings = serverStatuses = 0;
        }

        if (buf.isReadable()) {
            Charset charset = context.getClientCollation().getCharset();
            int sizeAfterVarInt = VarIntUtils.checkNextVarInt(buf);

            if (sizeAfterVarInt < 0) {
                return new OkMessage(isEndOfRows, affectedRows, lastInsertId, serverStatuses,
                    warnings, buf.toString(charset), Collections.emptyMap());
            }

            int oldReaderIndex = buf.readerIndex();
            long infoSize = VarIntUtils.readVarInt(buf);

            if (infoSize > sizeAfterVarInt) {
                // Compatible code, the information may be an EOF encoded string at early versions of MySQL.
                String info = buf.toString(oldReaderIndex, buf.writerIndex() - oldReaderIndex, charset);

                return new OkMessage(isEndOfRows, affectedRows, lastInsertId, serverStatuses, warnings,
                    info, Collections.emptyMap());
            }

            // All the following have lengths should be less than Integer.MAX_VALUE
            String information = buf.readCharSequence((int) infoSize, charset).toString();
            Map<String, String> systemVariables = Collections.emptyMap();

            while (VarIntUtils.checkNextVarInt(buf) >= 0) {
                int stateInfoSize = (int) VarIntUtils.readVarInt(buf);
                ByteBuf stateInfo = buf.readSlice(stateInfoSize);

                while (stateInfo.isReadable()) {
                    if (stateInfo.readByte() == SESSION_TRACK_SYSTEM_VARIABLES) {
                        systemVariables = readServerVariables(stateInfo, context);
                    } else {
                        // Ignore other state info
                        int skipBytes = (int) VarIntUtils.readVarInt(stateInfo);

                        stateInfo.skipBytes(skipBytes);
                    }
                }
            }

            // Ignore session track, it is not human-readable and useless for R2DBC client.
            return new OkMessage(isEndOfRows, affectedRows, lastInsertId, serverStatuses, warnings,
                information, systemVariables);
        }

        // Maybe have no human-readable message
        return new OkMessage(isEndOfRows, affectedRows, lastInsertId, serverStatuses, warnings, "",
            Collections.emptyMap());
    }

    private static Map<String, String> readServerVariables(ByteBuf buf, ConnectionContext context) {
        // All lengths should NOT be greater than Integer.MAX_VALUE
        Map<String, String> map = new HashMap<>();
        Charset charset = context.getClientCollation().getCharset();
        int size = (int) VarIntUtils.readVarInt(buf);
        ByteBuf sessionVar = buf.readSlice(size);

        while (sessionVar.readableBytes() > 0) {
            int variableSize = (int) VarIntUtils.readVarInt(sessionVar);
            String variable = sessionVar.toString(sessionVar.readerIndex(), variableSize, charset);

            sessionVar.skipBytes(variableSize);

            int valueSize = (int) VarIntUtils.readVarInt(sessionVar);
            String value = sessionVar.toString(sessionVar.readerIndex(), valueSize, charset);

            sessionVar.skipBytes(valueSize);
            map.put(variable, value);
        }

        switch (map.size()) {
            case 0:
                return Collections.emptyMap();
            case 1: {
                Map.Entry<String, String> entry = map.entrySet().iterator().next();
                return Collections.singletonMap(entry.getKey(), entry.getValue());
            }
            default:
                return map;
        }
    }
}

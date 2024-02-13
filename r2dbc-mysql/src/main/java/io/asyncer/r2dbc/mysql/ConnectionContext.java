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

package io.asyncer.r2dbc.mysql;

import io.asyncer.r2dbc.mysql.codec.CodecContext;
import io.asyncer.r2dbc.mysql.collation.CharCollation;
import io.asyncer.r2dbc.mysql.constant.ServerStatuses;
import io.asyncer.r2dbc.mysql.constant.ZeroDateOption;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;
import java.time.ZoneId;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * The MySQL connection context considers the behavior of server or client.
 * <p>
 * WARNING: Do NOT change any data outside of this project, try to configure {@code ConnectionFactoryOptions}
 * or {@code MySqlConnectionConfiguration} to control connection context and client behavior.
 */
public final class ConnectionContext implements CodecContext {

    private static final ServerVersion NONE_VERSION = ServerVersion.create(0, 0, 0);

    private volatile int connectionId = -1;

    private volatile ServerVersion serverVersion = NONE_VERSION;

    private final ZeroDateOption zeroDateOption;

    @Nullable
    private final Path localInfilePath;

    private final int localInfileBufferSize;

    private final boolean preserveInstants;

    @Nullable
    private ZoneId timeZone;

    /**
     * Assume that the auto commit is always turned on, it will be set after handshake V10 request message, or
     * OK message which means handshake V9 completed.
     */
    private volatile short serverStatuses = ServerStatuses.AUTO_COMMIT;

    @Nullable
    private volatile Capability capability = null;

    ConnectionContext(
        ZeroDateOption zeroDateOption,
        @Nullable Path localInfilePath,
        int localInfileBufferSize,
        boolean preserveInstants,
        @Nullable ZoneId timeZone
    ) {
        this.zeroDateOption = requireNonNull(zeroDateOption, "zeroDateOption must not be null");
        this.localInfilePath = localInfilePath;
        this.localInfileBufferSize = localInfileBufferSize;
        this.preserveInstants = preserveInstants;
        this.timeZone = timeZone;
    }

    /**
     * Get the connection identifier that is specified by server.
     *
     * @return the connection identifier.
     */
    public int getConnectionId() {
        return connectionId;
    }

    /**
     * Initializes this context.
     *
     * @param connectionId the connection identifier that is specified by server.
     * @param version      the server version.
     * @param capability   the connection capabilities.
     */
    public void init(int connectionId, ServerVersion version, Capability capability) {
        this.connectionId = connectionId;
        this.serverVersion = version;
        this.capability = capability;
    }

    @Override
    public ServerVersion getServerVersion() {
        return serverVersion;
    }

    @Override
    public CharCollation getClientCollation() {
        return CharCollation.clientCharCollation();
    }

    @Override
    public boolean isPreserveInstants() {
        return preserveInstants;
    }

    @Override
    public ZoneId getTimeZone() {
        if (timeZone == null) {
            throw new IllegalStateException("Server timezone have not initialization");
        }
        return timeZone;
    }

    public boolean isTimeZoneInitialized() {
        return timeZone != null;
    }

    @Override
    public boolean isMariaDb() {
        Capability capability = this.capability;
        return (capability != null && capability.isMariaDb()) || serverVersion.isMariaDb();
    }

    void setTimeZone(ZoneId timeZone) {
        if (isTimeZoneInitialized()) {
            throw new IllegalStateException("Server timezone have been initialized");
        }
        this.timeZone = timeZone;
    }

    @Override
    public ZeroDateOption getZeroDateOption() {
        return zeroDateOption;
    }

    /**
     * Gets the allowed local infile path.
     *
     * @return the path.
     */
    @Nullable
    public Path getLocalInfilePath() {
        return localInfilePath;
    }

    /**
     * Gets the local infile buffer size.
     *
     * @return the buffer size.
     */
    public int getLocalInfileBufferSize() {
        return localInfileBufferSize;
    }

    /**
     * Get the bitmap of server statuses.
     *
     * @return the bitmap.
     */
    public short getServerStatuses() {
        return serverStatuses;
    }

    /**
     * Updates server statuses.
     *
     * @param serverStatuses the bitmap of server statuses.
     */
    public void setServerStatuses(short serverStatuses) {
        this.serverStatuses = serverStatuses;
    }

    /**
     * Get the connection capability. Should use it after this context initialized.
     *
     * @return the connection capability.
     */
    public Capability getCapability() {
        return capability;
    }
}

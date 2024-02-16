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

import io.asyncer.r2dbc.mysql.constant.ServerStatuses;
import io.asyncer.r2dbc.mysql.constant.ZeroDateOption;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Unit tests for {@link ConnectionContext}.
 */
public class ConnectionContextTest {

    @Test
    void getServerZoneId() {
        for (int i = -12; i <= 12; ++i) {
            String id = i < 0 ? "UTC" + i : "UTC+" + i;
            ConnectionContext context = new ConnectionContext(
                ZeroDateOption.USE_NULL, null,
                8192, ZoneId.of(id));

            assertThat(context.getServerZoneId()).isEqualTo(ZoneId.of(id));
        }
    }

    @Test
    void shouldSetServerZoneId() {
        ConnectionContext context = new ConnectionContext(ZeroDateOption.USE_NULL, null,
            8192, null);
        assertThat(context.shouldSetServerZoneId()).isTrue();
        context.setServerZoneId(ZoneId.systemDefault());
        assertThat(context.shouldSetServerZoneId()).isFalse();
    }

    @Test
    void shouldNotSetServerZoneId() {
        ConnectionContext context = new ConnectionContext(ZeroDateOption.USE_NULL, null,
            8192, ZoneId.systemDefault());
        assertThat(context.shouldSetServerZoneId()).isFalse();
    }

    @Test
    void setTwiceServerZoneId() {
        ConnectionContext context = new ConnectionContext(ZeroDateOption.USE_NULL, null,
            8192, null);
        context.setServerZoneId(ZoneId.systemDefault());
        assertThatIllegalStateException().isThrownBy(() -> context.setServerZoneId(ZoneId.systemDefault()));
    }

    @Test
    void badSetServerZoneId() {
        ConnectionContext context = new ConnectionContext(ZeroDateOption.USE_NULL, null,
            8192, ZoneId.systemDefault());
        assertThatIllegalStateException().isThrownBy(() -> context.setServerZoneId(ZoneId.systemDefault()));
    }

    public static ConnectionContext mock() {
        return mock(false, ZoneId.systemDefault());
    }

    public static ConnectionContext mock(boolean isMariaDB) {
        return mock(isMariaDB, ZoneId.systemDefault());
    }

    public static ConnectionContext mock(boolean isMariaDB, ZoneId zoneId) {
        ConnectionContext context = new ConnectionContext(ZeroDateOption.USE_NULL, null,
            8192, zoneId);

        context.init(1, ServerVersion.parse(isMariaDB ? "11.2.22.MOCKED" : "8.0.11.MOCKED"),
            Capability.of(~(isMariaDB ? 1 : 0)));
        context.setServerStatuses(ServerStatuses.AUTO_COMMIT);

        return context;
    }
}

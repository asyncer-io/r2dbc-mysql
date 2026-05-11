/*
 * Copyright 2026 asyncer.io projects
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

import io.r2dbc.spi.Readable;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link InitFlow}.
 */
class InitFlowTest {

    @Test
    void readInnoDbLockWaitTimeout() {
        Optional<Duration> actual = InitFlow.readInnoDbLockWaitTimeout(row(
            "innodb_lock_wait_timeout", "5"));

        assertThat(actual).hasValue(Duration.ofSeconds(5));
    }

    @Test
    void ignoreUnrelatedVariableRows() {
        Optional<Duration> actual = InitFlow.readInnoDbLockWaitTimeout(row(
            "character_set_client", "utf8"));

        assertThat(actual).isEmpty();
    }

    @Test
    void ignoreMalformedLockWaitTimeout() {
        Optional<Duration> actual = InitFlow.readInnoDbLockWaitTimeout(row(
            "innodb_lock_wait_timeout", "utf8"));

        assertThat(actual).isEmpty();
    }

    @Test
    void ignoreEmptyLockWaitTimeout() {
        Optional<Duration> actual = InitFlow.readInnoDbLockWaitTimeout(row(
            "innodb_lock_wait_timeout", ""));

        assertThat(actual).isEmpty();
    }

    private static Readable row(String name, String value) {
        Readable readable = mock(Readable.class);

        when(readable.get(0, String.class)).thenReturn(name);
        when(readable.get(1, String.class)).thenReturn(value);

        return readable;
    }
}

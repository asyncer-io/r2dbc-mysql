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

import io.asyncer.r2dbc.mysql.collation.CharCollation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ColumnDefinition}.
 */
class ColumnDefinitionTest {

    @Test
    void allSet() {
        ColumnDefinition definition = ColumnDefinition.of(-1);

        assertThat(definition.isBinary()).isTrue();
        assertThat(definition.isSet()).isTrue();
        assertThat(definition.isUnsigned()).isTrue();
        assertThat(definition.isEnum()).isTrue();
        assertThat(definition.isNotNull()).isTrue();
    }

    @Test
    void noSet() {
        ColumnDefinition definition = ColumnDefinition.of(0);

        assertThat(definition.isBinary()).isFalse();
        assertThat(definition.isSet()).isFalse();
        assertThat(definition.isUnsigned()).isFalse();
        assertThat(definition.isEnum()).isFalse();
        assertThat(definition.isNotNull()).isFalse();

    }

    @Test
    void isBinaryUsesCollationId() {
        ColumnDefinition definition = ColumnDefinition.of(-1, CharCollation.BINARY_ID);

        assertThat(definition.isBinary()).isTrue();

        definition = ColumnDefinition.of(-1, ~CharCollation.BINARY_ID);
        assertThat(definition.isBinary()).isFalse();
    }
}

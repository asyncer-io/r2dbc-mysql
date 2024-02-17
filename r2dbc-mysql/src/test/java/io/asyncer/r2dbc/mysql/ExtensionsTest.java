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

import io.asyncer.r2dbc.mysql.extension.Extension;
import io.asyncer.r2dbc.mysql.json.JacksonCodecRegistrar;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link Extensions}.
 */
class ExtensionsTest {

    private final Extension[] manual = { mock(Extension.class), mock(Extension.class) };

    @Test
    void autodetect() {
        int size = manual.length + 1;
        assertThat(extract(Extensions.from(Arrays.asList(manual), true)))
            .hasSize(size)
            .startsWith(manual)
            .element(size - 1)
            .isInstanceOf(JacksonCodecRegistrar.class);
    }

    @Test
    void nonAutodetect() {
        assertThat(extract(Extensions.from(Arrays.asList(manual), false)))
            .hasSize(manual.length)
            .isEqualTo(Arrays.asList(manual));
    }

    private List<Extension> extract(Extensions extensions) {
        List<Extension> result = new ArrayList<>();

        extensions.forEach(Extension.class, result::add);

        return result;
    }
}

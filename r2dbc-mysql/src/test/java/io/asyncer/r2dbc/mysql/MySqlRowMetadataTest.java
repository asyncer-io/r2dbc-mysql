package io.asyncer.r2dbc.mysql;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MySqlRowMetadataTest {

    @ParameterizedTest
    @MethodSource("arguments")
    public void findColumnByNameTest(final String q, final int index, final String... names) {

        // given
        final MySqlRowMetadata metadata = create(names);

        // when
        if (index == -1) {
            assertThrows(NoSuchElementException.class, () -> metadata.getColumnMetadata(q));
            return;
        }
        final MySqlColumnDescriptor actual = metadata.getColumnMetadata(q);

        // then
        assertEquals(index, actual.getIndex());
    }

    private static Stream<Arguments> arguments() {
        return Stream.of(
                // not found
                Arguments.of("omega", -1,
                             new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),

                Arguments.of("alpha", 0, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("`alpha`", 0, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("Alpha", 0, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("`Alpha`", 2, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),

                Arguments.of("beta", 1, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("`beta`", 1, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("Beta", 1, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("`Beta`", -1, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),

                Arguments.of("delta", 3, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("`delta`", 6, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("Delta", 3, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("`Delta`", -1, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),

                Arguments.of("gamma", 4, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("`gamma`", 4, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("Gamma", 5, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" }),
                Arguments.of("`Gamma`", 5, new String[] { "alpha", "beta", "Alpha", "DELTA", "gamma", "Gamma", "delta" })
        );
    }

    private static MySqlRowMetadata create(final String... names) {
        MySqlColumnDescriptor[] metadata = new MySqlColumnDescriptor[names.length];
        for (int i = 0; i < names.length; ++i) {
            metadata[i] =
                    new MySqlColumnDescriptor(i, (short) 0, names[i], ColumnDefinition.of((short) 0, 1), 0, 0, 1);
        }
        return new MySqlRowMetadata(metadata);
    }

}
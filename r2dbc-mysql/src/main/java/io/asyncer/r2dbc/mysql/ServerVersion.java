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

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * MySQL server version, looks like {@literal "8.0.14"}, or {@literal "8.0.14-rc2"}.
 */
public final class ServerVersion implements Comparable<ServerVersion> {

    /**
     * MariaDB's replication hack prefix.
     * <p>
     * Note: MySQL 5.5.5 is not a stable version, so it should be safe.
     */
    private static final String MARIADB_RPL_HACK_PREFIX = "5.5.5-";

    private static final String ENTERPRISE = "enterprise";

    private static final String COMMERCIAL = "commercial";

    private static final String ADVANCED = "advanced";

    /**
     * Unresolved/origin version pattern, do NOT use it on {@link #hashCode()}, {@link #equals(Object)} or
     * {@link #compareTo(ServerVersion)}.
     */
    private final transient String origin;

    private final int major;

    private final int minor;

    private final int patch;

    private final boolean isMariaDb;

    private ServerVersion(String origin, int major, int minor, int patch, boolean isMariaDb) {
        this.origin = origin;
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.isMariaDb = isMariaDb;
    }

    /**
     * Returns whether the current {@link ServerVersion} is greater than or equal to the given one.
     *
     * @param version the give one.
     * @return if greater or the same as {@code version}.
     */
    public boolean isGreaterThanOrEqualTo(ServerVersion version) {
        return compareTo(version) >= 0;
    }

    /**
     * Returns whether the current {@link ServerVersion} is less than given one.
     *
     * @param version the give one
     * @return if less than {@code version}.
     */
    public boolean isLessThan(ServerVersion version) {
        return compareTo(version) < 0;
    }

    @Override
    public int compareTo(ServerVersion version) {
        // Standard `Comparable` must throw `NullPointerException` in `compareTo`,
        // so cannot use `AssertUtils.requireNonNull` in here (throws `IllegalArgumentException`).

        if (this.major != version.major) {
            return (this.major < version.major) ? -1 : 1;
        } else if (this.minor != version.minor) {
            return (this.minor < version.minor) ? -1 : 1;
        } else if (this.patch != version.patch) {
            return (this.patch < version.patch) ? -1 : 1;
        }

        return 0;
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getPatch() {
        return patch;
    }

    /**
     * Checks {@link ServerVersion this} contains MariaDB prefix or postfix.
     *
     * @return if it contains.
     */
    public boolean isMariaDb() {
        return isMariaDb;
    }

    /**
     * Checks if the version is enterprise edition.
     * <p>
     * Notice: it is unstable API, should not be used outer than {@literal r2dbc-mysql}.
     *
     * @return if is enterprise edition.
     */
    public boolean isEnterprise() {
        // Maybe should ignore case?
        return origin.contains(ENTERPRISE) || origin.contains(COMMERCIAL) || origin.contains(ADVANCED);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ServerVersion)) {
            return false;
        }

        ServerVersion that = (ServerVersion) o;

        return major == that.major && minor == that.minor && patch == that.patch;
    }

    @Override
    public int hashCode() {
        int hash = 31 * major + minor;
        return 31 * hash + patch;
    }

    @Override
    public String toString() {
        if (origin.isEmpty()) {
            return String.format("%d.%d.%d", major, minor, patch);
        }

        return origin;
    }

    /**
     * Parse a {@link ServerVersion} from {@link String}.
     *
     * @param version origin version string.
     * @return A {@link ServerVersion} that value decode from {@code version}.
     * @throws IllegalArgumentException if {@code version} is null, or any version part overflow.
     */
    public static ServerVersion parse(String version) {
        requireNonNull(version, "version must not be null");

        int length = version.length();
        int[] index = new int[] { 0 };
        boolean isMariaDb = false;

        if (version.startsWith(MARIADB_RPL_HACK_PREFIX)) {
            isMariaDb = true;
            index[0] = MARIADB_RPL_HACK_PREFIX.length();
        }

        int[] parts = new int[] { 0, 0, 0 };
        int i = 0;

        while (true) {
            parts[i] = readInt(version, length, index);

            if (index[0] >= length) {
                // End of version.
                break;
            }

            if (i == 2 || version.charAt(index[0]) != '.') {
                // End of version number parts, check postfix if needed.
                if (!isMariaDb) {
                    isMariaDb = version.indexOf("MariaDB", index[0]) >= 0;
                }

                break;
            }

            // Skip last '.' after current number part.
            ++index[0];
            ++i;
        }

        return create0(version, parts[0], parts[1], parts[2], isMariaDb);
    }

    /**
     * Create a {@link ServerVersion} that value is {@literal major.minor.patch}.
     *
     * @param major must not be a negative integer
     * @param minor must not be a negative integer
     * @param patch must not be a negative integer
     * @return A server version that value is {@literal major.minor.patch}
     * @throws IllegalArgumentException if any version part is negative integer.
     */
    public static ServerVersion create(int major, int minor, int patch) {
        return create0("", major, minor, patch, false);
    }

    /**
     * Create a {@link ServerVersion} that value is {@literal major.minor.patch} with MariaDB flag.
     *
     * @param major     must not be a negative integer
     * @param minor     must not be a negative integer
     * @param patch     must not be a negative integer
     * @param isMariaDb MariaDB flag
     * @return A server version that value is {@literal major.minor.patch}
     * @throws IllegalArgumentException if any version part is negative integer.
     */
    public static ServerVersion create(int major, int minor, int patch, boolean isMariaDb) {
        return create0("", major, minor, patch, isMariaDb);
    }

    private static ServerVersion create0(String origin, int major, int minor, int patch, boolean isMariaDb) {
        require(major >= 0, "major version must not be a negative integer");
        require(minor >= 0, "minor version must not be a negative integer");
        require(patch >= 0, "patch version must not be a negative integer");

        return new ServerVersion(origin, major, minor, patch, isMariaDb);
    }

    /**
     * C-style number parse, it most like {@code ByteBuf.read*(...)} and use external read index.
     *
     * @param input  the input string
     * @param length the size of {@code version}
     * @param index  reference of external read index, it will increase to the location of read
     * @return the integer read by {@code input}
     */
    private static int readInt(String input, int length, int[/* 1 */] index) {
        int ans = 0, i;
        char ch;

        for (i = index[0]; i < length; ++i) {
            ch = input.charAt(i);

            if (ch < '0' || ch > '9') {
                break;
            }

            ans = ans * 10 + (ch - '0');
        }

        index[0] = i;

        return ans;
    }
}

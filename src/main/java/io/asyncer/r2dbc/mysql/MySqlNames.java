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

/**
 * A utility considers column names searching logic which use a special compare rule for sort and special
 * binary search.
 *
 * <ul><li>Sort: compare with case insensitive first, then compare with case sensitive when they equals by
 * case insensitive.</li>
 * <li>Search: find with case sensitive first, then find with case insensitive when not found in case
 * sensitive.</li></ul>
 * <p>
 * For example:
 * Sort first: abc AB a Abc Ab ABC A ab b B -> A a B b AB Ab ab ABC Abc abc
 * Then find "aB" use the same compare rule,
 *
 * @see #compare(String, String)
 */
final class MySqlNames {

    /**
     * Find the best match of target string. This means that if it cannot find case-sensitive content, it will
     * try to find with case-insensitive. If the target string is enclosed by {@literal `} and contains at
     * least 1 character in quotes, it will find with case-sensitive only.
     *
     * @param names column names ordered by {@link #compare}.
     * @param name  the target string.
     * @return found index, or a negative integer means not found.
     */
    static int nameSearch(String[] names, String name) {
        int size = name.length();
        return binarySearch(names, name, size <= 2 || name.charAt(0) != '`' || name.charAt(size - 1) != '`');
    }

    private static int binarySearch(String[] names, String name, boolean ignoreCase) {
        int left = 0, right = names.length - 1, middle, compared;
        int nameStart = ignoreCase ? 0 : 1, nameEnd = ignoreCase ? name.length() : name.length() - 1;
        int ciResult = -1;
        String value;

        while (left <= right) {
            // `left + (right - left) / 2` for ensure no overflow,
            // `left + (right - left) / 2` = `(left + right) >>> 1`
            // when `left` and `right` is not negative integer.
            // And `left` must greater or equals than 0,
            // `right` greater then or equals to `left`.
            middle = (left + right) >>> 1;
            value = names[middle];
            compared = compare0(value, name, nameStart, nameEnd);

            if (compared < 0) {
                left = middle + 1;

                if (compared == -2) {
                    // Match succeed if case insensitive, always use last
                    // matched result that will be closer to `name`.
                    ciResult = middle;
                }
            } else if (compared > 0) {
                right = middle - 1;

                if (compared == 2) {
                    // Match succeed if case insensitive, always use last
                    // matched result that will be closer to `name`.
                    ciResult = middle;
                }
            } else {
                // Match succeed when case sensitive, just return.
                return middle;
            }
        }

        return ignoreCase ? ciResult : -1;
    }

    /**
     * Compares double strings and return an integer of both difference. If the integer is {@code 0} means
     * both strings equals even case sensitive, absolute value is {@code 2} means it is equals by case
     * insensitive but not equals when case sensitive, absolute value is {@code 4} means it is not equals even
     * case insensitive.
     * <p>
     * Note: visible for unit tests.
     *
     * @param left  the {@link String} of left.
     * @param right the {@link String} of right.
     * @return an integer of both difference.
     */
    static int compare(String left, String right) {
        return compare0(left, right, 0, right.length());
    }

    private static int compare0(String left, String right, int start, int end) {
        int leftSize = left.length(), rightSize = end - start;
        int minSize = Math.min(leftSize, rightSize);
        // Case sensitive comparator result.
        int csCompared = 0;
        char leftCh, rightCh;

        for (int i = 0; i < minSize; i++) {
            leftCh = left.charAt(i);
            rightCh = right.charAt(i + start);

            if (leftCh != rightCh) {
                if (csCompared == 0) {
                    // Compare end if is case sensitive comparator.
                    csCompared = leftCh - rightCh;
                }

                // Use `Character.toLowerCase` for all latin alphabets, not just ASCII.
                leftCh = Character.toLowerCase(leftCh);
                rightCh = Character.toLowerCase(rightCh);

                if (leftCh != rightCh) {
                    // Not equals even case insensitive.
                    return leftCh < rightCh ? -4 : 4;
                }
            }
        }

        // Length not equals means both strings not equals even case insensitive.
        if (leftSize != rightSize) {
            return leftSize < rightSize ? -4 : 4;
        }

        // Equals when case insensitive, use case sensitive.
        return csCompared < 0 ? -2 : (csCompared > 0 ? 2 : 0);
    }

    private MySqlNames() { }
}

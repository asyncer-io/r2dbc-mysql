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
 * Integration tests for {@link MySqlTestKitSupport} with binary protocol.
 */
class MySqlPrepareTestKit extends MySqlTestKitSupport {

    MySqlPrepareTestKit() {
        super(IntegrationTestSupport.configuration("r2dbc", false, false, null, sql -> true));
    }

    @Override
    public void compoundStatement() {
        // MySQL does not support multiple preparing results.
    }
}

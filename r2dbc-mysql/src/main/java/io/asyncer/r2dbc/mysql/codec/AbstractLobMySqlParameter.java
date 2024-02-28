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

package io.asyncer.r2dbc.mysql.codec;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Base class considers LOB types (i.e. BLOB, CLOB) for {@link AbstractMySqlParameter} implementations.
 */
abstract class AbstractLobMySqlParameter extends AbstractMySqlParameter {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AbstractLobMySqlParameter.class);

    @Override
    public final void dispose() {
        try {
            Publisher<Void> discard = getDiscard();

            if (discard == null) {
                return;
            }

            if (discard instanceof Mono<?>) {
                ((Mono<?>) discard).subscribe(null, e ->
                    logger.error("Exception happened in LOB type cancel binding", e));
            } else {
                Flux.from(discard).subscribe(null, e ->
                    logger.error("Exception happened in LOB type cancel binding", e));
            }
        } catch (Exception e) {
            logger.error("Exception happened in LOB type cancel binding", e);
        }
    }

    @Nullable
    protected abstract Publisher<Void> getDiscard();
}

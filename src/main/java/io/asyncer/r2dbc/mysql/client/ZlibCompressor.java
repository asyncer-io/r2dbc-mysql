/*
 * Copyright 2024 asyncer.io projects
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

package io.asyncer.r2dbc.mysql.client;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderException;

import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * An implementation of {@link Compressor} that uses the zlib compression algorithm.
 *
 * @see io.netty.handler.codec.compression Netty Compression Codecs
 */
final class ZlibCompressor implements Compressor {

    /**
     * The maximum size of input buffer and the maximum initial capacity of the compressed data buffer.
     * <p>
     * Note: uncompressed size is already known, so the buffer should be allocated with the exact size.
     */
    private static final int MAX_CHUNK_SIZE = 65536;

    private final Deflater deflater = new Deflater();

    private final Inflater inflater = new Inflater();

    @Override
    public ByteBuf compress(ByteBuf buf) {
        int len = buf.readableBytes();

        if (len == 0) {
            return buf.alloc().buffer(0, 0);
        }

        try {
            if (buf.hasArray()) {
                byte[] input = buf.array();
                int offset = buf.arrayOffset() + buf.readerIndex();
                ByteBuf out = buf.alloc().heapBuffer(Math.min(len, MAX_CHUNK_SIZE));

                deflater.setInput(input, offset, len);
                deflater.finish();
                deflateAll(out, len);

                return out;
            } else {
                byte[] input = new byte[Math.min(len, MAX_CHUNK_SIZE)];
                int readerIndex = buf.readerIndex();
                int writerIndex = buf.writerIndex();
                ByteBuf out = buf.alloc().heapBuffer(Math.min(len, MAX_CHUNK_SIZE));

                while (writerIndex - readerIndex > 0) {
                    int numBytes = Math.min(input.length, writerIndex - readerIndex);

                    buf.getBytes(readerIndex, input, 0, numBytes);
                    deflater.setInput(input, 0, numBytes);
                    readerIndex += numBytes;
                    deflateAll(out, len);
                }

                deflater.finish();
                deflateAll(out, len);

                return out;
            }
        } finally {
            deflater.reset();
        }
    }

    @Override
    public ByteBuf decompress(ByteBuf buf, int uncompressedSize) {
        int len = buf.readableBytes();

        if (len == 0) {
            return buf.alloc().buffer(0, 0);
        }

        try {
            if (buf.hasArray()) {
                byte[] input = buf.array();
                int offset = buf.arrayOffset() + buf.readerIndex();
                ByteBuf out = buf.alloc().heapBuffer(uncompressedSize);

                inflater.setInput(input, offset, len);
                inflateAll(out);

                return out;
            } else {
                byte[] input = new byte[Math.min(len, MAX_CHUNK_SIZE)];

                int readerIndex = buf.readerIndex();
                int writerIndex = buf.writerIndex();
                ByteBuf out = buf.alloc().heapBuffer(uncompressedSize);

                while (writerIndex - readerIndex > 0) {
                    int numBytes = Math.min(input.length, writerIndex - readerIndex);

                    buf.getBytes(readerIndex, input, 0, numBytes);
                    inflater.setInput(input, 0, numBytes);
                    readerIndex += numBytes;
                    inflateAll(out);
                }

                return out;
            }
        } catch (DataFormatException e) {
            throw new DecoderException("zlib decompress failed", e);
        } finally {
            inflater.reset();
        }
    }

    @Override
    public void dispose() {
        deflater.end();
        inflater.end();
    }

    private void deflateAll(ByteBuf out, int maxSize) {
        while (true) {
            deflate(out);

            if (!out.isWritable()) {
                int size = out.readableBytes();

                if (size >= maxSize) {
                    break;
                }

                // Capacity = written size * 2
                if (size > (maxSize >> 1)) {
                    out.ensureWritable(maxSize - size);
                } else {
                    out.ensureWritable(size);
                }
            } else if (deflater.needsInput()) {
                break;
            }
        }
    }

    private void inflateAll(ByteBuf out) throws DataFormatException {
        while (out.isWritable() && !inflater.finished()) {
            int wid = out.writerIndex();
            int numBytes = inflater.inflate(out.array(), out.arrayOffset() + wid, out.writableBytes());

            out.writerIndex(wid + numBytes);
        }
    }

    private void deflate(ByteBuf out) {
        int wid = out.writerIndex();
        int written = deflater.deflate(out.array(), out.arrayOffset() + wid, out.writableBytes());

        while (written > 0) {
            wid += written;
            out.writerIndex(wid);
            written = deflater.deflate(out.array(), out.arrayOffset() + wid, out.writableBytes());
        }
    }
}

package org.reaktivity.nukleus.http_cache.internal.stream.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW.Builder;

public class OctetsFWInflaterTest
{

    final MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[2048]);
    final MutableDirectBuffer writeBuffer2 = new UnsafeBuffer(new byte[2048]);

    @Test
    public void shouldInflateGzip() throws DataFormatException, IOException
    {
        final int transferSize = 50;
        final byte[] expected = "hello world".getBytes();
        final DirectBufferInflater inflator = new DirectBufferInflater(transferSize);
        byte[] compressed = gzipDeflate(expected);

        Assert.assertTrue(compressed.length < transferSize);
        OctetsFW fw = new OctetsFW.Builder()
                .wrap(writeBuffer, 0, 2048)
                .set(compressed)
                .build();
        Builder builder = new OctetsFW.Builder().wrap(writeBuffer2, 0, 2048);

        OctetsFW fw2 = inflator.inflate(builder, fw, 0, fw.sizeof());

        byte[] actual = new byte[expected.length];
        fw2.buffer().getBytes(fw2.offset(), actual, 0, fw2.sizeof());
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void shouldInflateGzipMultipleChunksMultipleTimes() throws DataFormatException, IOException
    {
        final byte[] expected = "hello world and mars".getBytes();
        final int transferSize = 10;
        final DirectBufferInflater inflator = new DirectBufferInflater(transferSize);

        for (int iterations = 0; iterations < 10; iterations++)
        {
            byte[] compressed = gzipDeflate(expected);

            Assert.assertTrue(compressed.length > transferSize);
            Builder builder = new OctetsFW.Builder().wrap(writeBuffer2, 0, 2048);
            builder.reset();

            byte[] actual = new byte[expected.length];
            int writeToIndex = 0;
            int readFromIndex = 0;
            while (writeToIndex != expected.length)
            {
                int toCopy = Math.min(transferSize, compressed.length - readFromIndex);
                OctetsFW fw = new OctetsFW.Builder()
                        .wrap(writeBuffer, 0, transferSize)
                        .set(Arrays.copyOfRange(compressed, readFromIndex, readFromIndex + toCopy))
                        .build();
                OctetsFW fw2 = inflator.inflate(builder, fw, 0, fw.sizeof());
                final int offset = fw2.offset();
                final int sizeof = fw2.sizeof();
                fw2.buffer().getBytes(offset, actual, writeToIndex, sizeof);
                writeToIndex += sizeof;
                readFromIndex += toCopy;
            }

            Assert.assertArrayEquals(expected, actual);
        }
    }

    private static byte[] gzipDeflate(byte[] input) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);
        Deflater deflator = new Deflater();
        deflator.setLevel(Deflater.BEST_COMPRESSION);

        deflator.setInput(input);
        deflator.finish();

        byte[] buf = new byte[1024];
        while (!deflator.finished())
        {
            int count = deflator.deflate(buf);
            bos.write(buf, 0, count);
        }
        bos.close();

        return bos.toByteArray();
    }

    public static byte[] gzipInflate(
        byte[] compressedData,
        int off,
        int len) throws IOException, DataFormatException
    {
        Inflater decompressor = new Inflater();
        decompressor.setInput(compressedData, off, len);

        ByteArrayOutputStream bos = new ByteArrayOutputStream(compressedData.length);

        byte[] buf = new byte[1024];
        while (!decompressor.finished())
        {
            int count = decompressor.inflate(buf);
            bos.write(buf, 0, count);
        }
        bos.close();

        return bos.toByteArray();
    }

}

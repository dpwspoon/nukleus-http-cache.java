package org.reaktivity.nukleus.http_cache.internal.stream.util;

import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW.Builder;

public class DirectBufferInflater
{
    private final Inflater inflator;
    private final byte[] transferIn;
    private final byte[] transferOut;
    private final int transferSize;

    public DirectBufferInflater(int transferSize)
    {
        this.inflator = new Inflater();
        this.transferSize = transferSize;
        this.transferIn = new byte[transferSize];
        this.transferOut = new byte[transferSize];
    }

    public OctetsFW inflate(
        Builder builder,
        OctetsFW fw,
        int index,
        int offset) throws DataFormatException
    {
        int totalWrite = fw.sizeof();
        int written = 0;
        while (written < totalWrite)
        {
            int toWrite = Math.min(transferSize, totalWrite - written);
            fw.buffer().getBytes(0, transferIn, fw.offset() + written, toWrite);
            inflator.setInput(transferIn, 0, toWrite);
            do
            {
                int read = inflator.inflate(transferOut, 0, transferSize);
                final byte[] copyOf = Arrays.copyOf(transferOut, read);
                builder.set(copyOf);
            }
            while (!inflator.needsInput());
            written += toWrite;
        }
        if (inflator.finished())
        {
            inflator.reset();
        }
        return builder.build();
    }

}

package org.reaktivity.nukleus.http_cache.internal.stream.cache2;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;

public class ResponseStore
{

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final ListFW<HttpHeaderFW> requestRO = new HttpBeginExFW().headers();

    private final BufferPool bufferPool;
    private final Long2LongHashMap slotToReferenceCnt;
    private final Long2LongHashMap slotToBeginFrameSize;
    private final Long2LongHashMap slotToSize;

    public ResponseStore(BufferPool bufferPool)
    {
        this.bufferPool = bufferPool;
        this.slotToReferenceCnt = new Long2LongHashMap(-1);
        this.slotToBeginFrameSize = new Long2LongHashMap(-1);
        this.slotToSize = new Long2LongHashMap(-1);
    }

    public int store(BeginFW begin)
    {
        // TODO fix hash collision
        final int slot = bufferPool.acquire(0);
        slotToReferenceCnt.put(slot, 1);
        slotToSize.put(slot, 0);
        slotToBeginFrameSize.put(slot, 0);

        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
        final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();

        // Purposeful no NO_SLOT check, should hard fail
        MutableDirectBuffer buffer = bufferPool.buffer(slot);
        responseHeaders.forEach(h ->
        {
            final int size = (int) slotToSize.get(slot);
            buffer.putBytes(size, h.buffer(), h.offset(), h.sizeof());
            final int totalSize = size + h.sizeof();
            slotToSize.put(slot, totalSize);
            slotToBeginFrameSize.put(slot, totalSize);
        });
        return slot;
    }

    public boolean store(DataFW data, int slot)
    {
        MutableDirectBuffer buffer = bufferPool.buffer(slot);
        OctetsFW payload = data.payload();
        int sizeOfPayload = payload.sizeof();
        final int size = (int) slotToSize.get(slot);

        if (size + sizeOfPayload + 4 > bufferPool.slotCapacity())
        {
            return false;
        }
        else
        {
            buffer.putBytes(size, payload.buffer(), payload.offset(), sizeOfPayload);
            slotToSize.put(slot, sizeOfPayload + size);
            return true;
        }
    }
    public boolean store(EndFW end, int slot)
    {
        // TODO, trailing headers..
        return true;
    }

    public ListFW<HttpHeaderFW> getHeaders(int slot)
    {
        MutableDirectBuffer buffer = bufferPool.buffer(slot);
        final int size = (int) slotToBeginFrameSize.get(slot);
        return requestRO.wrap(buffer, 0, size);
    }

    public ListFW<HttpHeaderFW> getPalyoad(int slot)
    {
        MutableDirectBuffer buffer = bufferPool.buffer(slot);
        final int beginFrameSize = (int) slotToBeginFrameSize.get(slot);
        final int size = (int) slotToSize.get(slot);
        return requestRO.wrap(buffer, beginFrameSize, size);
    }

    public void release(int slot)
    {
        long newValue = slotToReferenceCnt.get(slot) - 1;
        if(newValue == 0)
        {
            bufferPool.release(slot);
            slotToReferenceCnt.remove(slot);
            slotToSize.remove(slot);
            slotToBeginFrameSize.remove(slot);
        }
        else
        {
            slotToReferenceCnt.put(slot, newValue);
        }
    }
}

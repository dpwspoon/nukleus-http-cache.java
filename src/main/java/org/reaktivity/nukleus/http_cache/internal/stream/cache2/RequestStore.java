package org.reaktivity.nukleus.http_cache.internal.stream.cache2;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;

public class RequestStore
{
    private final ListFW<HttpHeaderFW> requestRO = new HttpBeginExFW().headers();
    private final BufferPool bufferPool;
    private final Long2LongHashMap slotToReferenceCnt;
    private final Long2LongHashMap slotToSize;

    public RequestStore(BufferPool bufferPool)
    {
        this.bufferPool = bufferPool;
        this.slotToReferenceCnt = new Long2LongHashMap(-1);
        this.slotToSize = new Long2LongHashMap(-1);
    }

    public int store(ListFW<HttpHeaderFW> requestHeaders)
    {
        // TODO fix hash collision
        final int slot = bufferPool.acquire(0);
        slotToReferenceCnt.put(slot, 1);
        slotToSize.put(slot, 0);

        // Purposeful no NO_SLOT check, should hard fail
        MutableDirectBuffer buffer = bufferPool.buffer(slot);
        requestHeaders.forEach(h ->
        {
            final int size = (int) slotToSize.get(slot);
            buffer.putBytes(size, h.buffer(), h.offset(), h.sizeof());
            slotToSize.put(slot, size + h.sizeof());
        });
        return slot;
    }

    public ListFW<HttpHeaderFW> get(int slot)
    {
        MutableDirectBuffer buffer = bufferPool.buffer(slot);
        final int size = (int) slotToSize.get(slot);
        return requestRO.wrap(buffer, 0, size);
    }

    public void release(int slot)
    {
        long newValue = slotToReferenceCnt.get(slot) - 1;
        if(newValue == 0)
        {
            bufferPool.release(slot);
            slotToReferenceCnt.remove(slot);
            slotToSize.remove(slot);
        }
        else
        {
            slotToReferenceCnt.put(slot, newValue);
        }
    }
}

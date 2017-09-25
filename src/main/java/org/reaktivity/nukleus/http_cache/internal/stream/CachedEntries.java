package org.reaktivity.nukleus.http_cache.internal.stream;

import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Consumer;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

class CachedEntries
{
    private LinkedList<Consumer<MessageConsumer>> entryQueue = new LinkedList<>();

    public Optional<Consumer<MessageConsumer>> matchFirst(ListFW<HttpHeaderFW> requestHeaders)
    {
        return entryQueue.stream()
            .filter(entry -> responseCanServeRequest(entry))
            .findFirst();
        // TODO Auto-generated method stub
    }

    class CacheEntry implements Consumer<MessageConsumer>
    {
        private int requestSlot;
        private int responseSlot;
        private int numOfReaders = 0;
        private boolean purging = false;

        @Override
        public void accept(MessageConsumer t)
        {
            // TODO Auto-generated method stub
        }
    }

    private boolean responseCanServeRequest(Consumer<MessageConsumer> cacheEntry)
    {
        return false;
    }

}
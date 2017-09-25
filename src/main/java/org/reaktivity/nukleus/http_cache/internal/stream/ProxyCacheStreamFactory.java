package org.reaktivity.nukleus.http_cache.internal.stream;

import static java.lang.Integer.parseInt;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.Correlation;
import org.reaktivity.nukleus.http_cache.internal.stream.ProxyStreamFactory.ProxyAcceptStream;
import org.reaktivity.nukleus.http_cache.internal.stream.cache2.RequestStore;
import org.reaktivity.nukleus.http_cache.internal.stream.cache2.ResponseStore;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public class ProxyCacheStreamFactory
{
    final Long2ObjectHashMap<ProxyPollingConnectStream> cacheKeyToPollingConnectStreams;
    final Long2ObjectHashMap<CachedEntries> cacheKeyToCachedResponses;
    final RequestStore requestStore;
    final ResponseStore responseStore;
    final LongSupplier supplyCorrelationId;
    final LongObjectBiConsumer<Runnable> scheduler;
    final Long2ObjectHashMap<Correlation> correlations;
    final LongSupplier supplyStreamId;
    final ConnectStreamFactory proxyConnectStreamFactory;
    final Writer writer;

    public ProxyCacheStreamFactory(
        ConnectStreamFactory proxyConnectStreamFactory,
        LongSupplier supplyCorrelationId,
        LongObjectBiConsumer<Runnable> scheduler,
        BufferPool bufferPool,
        Long2ObjectHashMap<Correlation> correlations,
        LongSupplier supplyStreamId,
        Writer writer)
    {
        this.proxyConnectStreamFactory = proxyConnectStreamFactory;
        this.supplyCorrelationId = supplyCorrelationId;
        this.scheduler = scheduler;
        this.correlations = correlations;
        this.supplyStreamId = supplyStreamId;
        this.writer = writer;

        this.requestStore = new RequestStore(bufferPool);
        this.responseStore = new ResponseStore(bufferPool);

        cacheKeyToPollingConnectStreams = new Long2ObjectHashMap<>();
        cacheKeyToCachedResponses = new Long2ObjectHashMap<>();
    }

    public void subscribeToCacheUpdate(
        ProxyAcceptStream proxyAcceptStream,
        ListFW<HttpHeaderFW> requestHeaders,
        MessageConsumer acceptReply,
        MessageConsumer connect)
    {
        if (false /*alreadyOutOfDate - TODO optimization*/)
        {
            serviceRequest(proxyAcceptStream, requestHeaders, acceptReply, connect);
        }
        else
        {
            final String connectName = proxyAcceptStream.connectName;
            final long connectRef = proxyAcceptStream.connectRef;

            long cacheKey = getCacheKey(requestHeaders);

            // TODO, move polling interval to configuration
            long pollingInterval = parseInt(getHeader(requestHeaders, "x-retry-after"));

            ProxyPollingConnectStream pollingConnectStream = cacheKeyToPollingConnectStreams
                    .computeIfAbsent(
                            cacheKey,
                            l -> new ProxyPollingConnectStream(
                                    this,
                                    connectName,
                                    connect,
                                    connectRef,
                                    requestHeaders,
                                    pollingInterval));

            long requestKey = requestStore.store(requestHeaders);
            pollingConnectStream.subscribeToUpdate(requestKey);
        }
    }

    public void serviceRequest(
        ProxyAcceptStream proxyAcceptStream,
        ListFW<HttpHeaderFW> requestHeaders,
        MessageConsumer acceptReply,
        MessageConsumer connect)
    {
        // TODO Optimization, create pollingConnectStream based on configuration or first request...

        final long cacheKey = getCacheKey(requestHeaders);
        final CachedEntries cachedResponses = cacheKeyToCachedResponses.get(cacheKey);
        Consumer<MessageConsumer> fetchConnect = null;
        final Optional<Consumer<MessageConsumer>> cachedResponse =
                cachedResponses.matchFirst(requestHeaders);
        if (cachedResponse.isPresent())
        {
            // TODO use cache
        }
        else
        {
            fetchAndCache(proxyAcceptStream, requestHeaders, acceptReply, connect);
        }
        // TODO
//        cachedResponse.accept(proxyAcceptStream::handleResponseFromCache);
    }

    private Consumer<MessageConsumer> fetchAndCache(
        ProxyAcceptStream proxyAcceptStream,
        ListFW<HttpHeaderFW> requestHeaders,
        MessageConsumer acceptReply,
        MessageConsumer connect)
    {

        return new ProxyCacheInitConnectStream(
                this,
                connect,
                acceptReply,
                requestHeaders,
                proxyAcceptStream);
    }

    private static long getCacheKey(
        ListFW<HttpHeaderFW> requestHeaders)
    {
        return getRequestURL(requestHeaders).hashCode();
    }

}

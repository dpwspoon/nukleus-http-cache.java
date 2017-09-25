package org.reaktivity.nukleus.http_cache.internal.stream;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.Correlation;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public class ProxyPollingConnectStream
{

    private final ProxyCacheStreamFactory cacheStreamFactory;
    private final String connectName;
    private final MessageConsumer connect;
    private final long connectRef;
    private final ListFW<HttpHeaderFW> requestHeaders;
    private long intervalInMs;

    private int headersSize = 0;

    public ProxyPollingConnectStream(
        ProxyCacheStreamFactory cacheStreamFactory,
        String connectName,
        MessageConsumer connect,
        long connectRef,
        ListFW<HttpHeaderFW> requestHeaders,
        long intervalInMs)
    {
        this.cacheStreamFactory = cacheStreamFactory;
        this.connectName = connectName;
        this.connect = connect;
        this.connectRef = connectRef;
        this.requestHeaders = requestHeaders;
        this.intervalInMs = intervalInMs;

        cacheStreamFactory.requestStore.store(requestHeaders);

        cacheStreamFactory.scheduler.accept(intervalInMs, this::poll);
    }


    private void poll()
    {
        long correlationId = cacheStreamFactory.supplyCorrelationId.getAsLong();
        Correlation correlation = new Correlation(0, this::handleResponse, null, 0, 0, false, null, 0);
        cacheStreamFactory.correlations.put(correlationId, correlation);
    }

    private void handleResponse(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        // TODO
    }

    public void subscribeToUpdate(
        long requestKey)
    {
        // TODO Auto-generated method stub
    }
}
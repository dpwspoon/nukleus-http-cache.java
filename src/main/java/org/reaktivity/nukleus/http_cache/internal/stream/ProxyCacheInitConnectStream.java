package org.reaktivity.nukleus.http_cache.internal.stream;

import java.util.function.Consumer;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.ProxyStreamFactory.ProxyAcceptStream;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;

public class ProxyCacheInitConnectStream implements Consumer<MessageConsumer>
{

    private final int requestSlot;

    public ProxyCacheInitConnectStream(
        ProxyCacheStreamFactory cacheStreamFactory,
        MessageConsumer connect,
        MessageConsumer acceptReply,
        ListFW<HttpHeaderFW> requestHeaders,
        ProxyAcceptStream proxyAcceptStream)
    {
        this.requestSlot = cacheStreamFactory.requestStore.store(requestHeaders);
        MessageConsumer acceptReplySpy = (msgTypeId, buffer, index, length) ->
        {
            switch(msgTypeId)
            {
                case BeginFW.TYPE_ID:
                    break;
                case DataFW.TYPE_ID:
                    break;
                case EndFW.TYPE_ID:
                    cacheStreamFactory.responseStore.release(responseSlot);
                    break;
                default:
                    // abort
                    cacheStreamFactory.responseStore.release(responseSlot);
            }
        };

        MessageConsumer connectProxy = cacheStreamFactory.proxyConnectStreamFactory.proxy(
                proxyAcceptStream,
                requestHeaders,
                connect,
                acceptReplySpy);
        cacheStreamFactory.writer.doHttpEnd(connectProxy, proxyAcceptStream.connectStreamId);
    }

    @Override
    public void accept(MessageConsumer t)
    {
        // TODO Auto-generated method stub
    }

}

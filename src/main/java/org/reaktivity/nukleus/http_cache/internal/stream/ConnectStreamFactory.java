package org.reaktivity.nukleus.http_cache.internal.stream;

import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http_cache.internal.Correlation;
import org.reaktivity.nukleus.http_cache.internal.stream.ProxyStreamFactory.ProxyAcceptStream;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;

public class ConnectStreamFactory
{

    private final BeginFW beginRO = new BeginFW();
    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Long2ObjectHashMap<Correlation> correlations;
    private final LongSupplier supplyCorrelationId;
    private final LongSupplier supplyStreamId;
    private final Writer writer;
    private RouteManager router;

    public ConnectStreamFactory(
        LongSupplier supplyCorrelationId,
        LongSupplier supplyStreamId,
        Long2ObjectHashMap<Correlation> correlations,
        Writer writer,
        RouteManager router)
    {
        this.supplyCorrelationId = supplyCorrelationId;
        this.supplyStreamId = supplyStreamId;
        this.correlations = correlations;
        this.writer = writer;
        this.router = router;
    }

    public MessageConsumer proxy(
        ProxyAcceptStream proxyAcceptStream,
        ListFW<HttpHeaderFW> requestHeaders,
        MessageConsumer connect,
        MessageConsumer acceptReply)
    {
        ProxyConnectStream connectStream =
            new ProxyConnectStream(
                proxyAcceptStream,
                requestHeaders,
                acceptReply,
                connect);
        return connectStream::handleAcceptAfterProxyBegin;
    }

    public class ProxyConnectStream
    {
        private final MessageConsumer connect;
        private final long connectStreamId;
        private ProxyAcceptStream proxyAcceptStream;
        private long acceptReplyStreamId;
        private long connectReplyStreamId;
        private MessageConsumer connectReplyThrottle;
        private final MessageConsumer acceptReply;

        public ProxyConnectStream(
            ProxyAcceptStream proxyAcceptStream,
            ListFW<HttpHeaderFW> requestHeaders,
            MessageConsumer acceptReply,
            MessageConsumer connect)
        {
            this.acceptReply = acceptReply;
            this.proxyAcceptStream = proxyAcceptStream;
            final String connectName = proxyAcceptStream.connectName;
            this.connect = connect;
            final long connectRef = proxyAcceptStream.connectRef;
            this.connectStreamId = supplyStreamId.getAsLong();
            final long connectCorrelationId = supplyCorrelationId.getAsLong();

            Correlation correlation = new Correlation(this::handleConnectReply,
                    (connectReplyThrottle) -> this.connectReplyThrottle = connectReplyThrottle);
            correlations.put(connectCorrelationId, correlation);

            writer.doHttpBegin(connect, connectStreamId, connectRef, connectCorrelationId, e ->
                requestHeaders.forEach(h ->
                    e.item(h2 -> h2.representation((byte) 0).name(h.name())
                                .value(h.value()))
                )
            );

           router.setThrottle(connectName, connectStreamId, proxyAcceptStream::handleConnectThrottle);
        }

        private void handleAcceptAfterProxyBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleAcceptData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleAcceptEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAcceptAbort(abort);
                break;
            default:
                writer.doReset(proxyAcceptStream.acceptThrottle, proxyAcceptStream.acceptStreamId);
                writer.doAbort(connect, connectStreamId);
                break;
            }
        }

        private void handleAcceptData(
                DataFW data)
        {
            final OctetsFW payload = data.payload();
            writer.doHttpData(connect, connectStreamId, payload.buffer(), payload.offset(), payload.sizeof());
        }

        private void handleAcceptEnd(
                EndFW end)
        {
            writer.doHttpEnd(connect, connectStreamId);
        }

        private void handleAcceptAbort(
                AbortFW abort)
        {
            writer.doAbort(connect, connectStreamId);
        }

        void handleConnectReply(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            long acceptCorrelationId = proxyAcceptStream.acceptCorrelationId;
            switch (msgTypeId)
            {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    final OctetsFW extension = begin.extension();
                    final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
                    final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();
                    final String acceptReplyName = proxyAcceptStream.acceptReplyName;

                    this.connectReplyStreamId = begin.streamId();
                    this.acceptReplyStreamId = supplyStreamId.getAsLong();
                    writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e ->
                    responseHeaders.forEach(h -> e.item(
                        h2 -> h2.representation((byte) 0)
                        .name(h.name())
                        .value(h.value())))
                    );
                    router.setThrottle(acceptReplyName, acceptReplyStreamId, this::handleAcceptReplyThrottle);
                    break;
                case DataFW.TYPE_ID:
                    dataRO.wrap(buffer, index, index + length);
                    OctetsFW payload = dataRO.payload();
                    writer.doHttpData(acceptReply, acceptReplyStreamId, payload.buffer(), payload.offset(), payload.sizeof());
                    break;
                case EndFW.TYPE_ID:
                    writer.doHttpEnd(acceptReply, acceptReplyStreamId);
                    break;
                case AbortFW.TYPE_ID:
                    writer.doAbort(acceptReply, acceptReplyStreamId);
                    break;
                default:
                    break;
            }
        }

        private void handleAcceptReplyThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleAcceptReplyWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleAcceptReplyReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleAcceptReplyWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();

            writer.doWindow(connectReplyThrottle, this.connectReplyStreamId, bytes, frames);
        }

        private void handleAcceptReplyReset(
            ResetFW reset)
        {
            writer.doReset(connectReplyThrottle, this.connectReplyStreamId);
        }
    }
}

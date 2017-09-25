/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http_cache.internal.stream;

import static java.lang.Integer.parseInt;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpCacheUtils.canInjectPushPromise;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpCacheUtils.isPrivateCacheableResponse;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpCacheUtils.requestShouldBypassCache;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpCacheUtils.requestWantsResponseOnCacheUpdate;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CONTENT_LENGTH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.X_HTTP_CACHE_SYNC;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.X_POLL_INJECTED;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.INJECTED_HEADER_AND_NO_CACHE;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.INJECTED_HEADER_AND_NO_CACHE_VALUE;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.NO_CACHE_CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.SHOULD_POLL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getRequestURL;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http_cache.internal.Correlation;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Cache;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Cache.CacheResponseServer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.CacheDirectives;
import org.reaktivity.nukleus.http_cache.internal.stream.util.GroupThrottle;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpCacheUtils;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders;
import org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil;
import org.reaktivity.nukleus.http_cache.internal.stream.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.Flyweight;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW.Builder;
import org.reaktivity.nukleus.http_cache.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_cache.internal.types.String16FW;
import org.reaktivity.nukleus.http_cache.internal.types.StringFW;
import org.reaktivity.nukleus.http_cache.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public class ProxyStreamFactory implements StreamFactory
{

    
    private static final String IF_UNMODIFIED_SINCE = "if-unmodified-since";
    private static final String STALE_WHILE_REVALIDATE_2147483648 = "stale-while-revalidate=2147483648";

    // TODO, remove need for RW in simplification of inject headers
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final BeginFW beginRO = new BeginFW();
    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final ListFW<HttpHeaderFW> requestHeadersRO = new HttpBeginExFW().headers();
    private final ListFW<HttpHeaderFW> pendingRequestHeadersRO = new HttpBeginExFW().headers();
    private final DataFW dataRO = new DataFW();
    private final OctetsFW octetsRO = new OctetsFW();
    private final EndFW endRO = new EndFW();
    private final RouteFW routeRO = new RouteFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();
    private final AbortFW abortRO = new AbortFW();

    private final RouteManager router;

    private final LongSupplier supplyStreamId;
    private final BufferPool streamBufferPool;
    private final BufferPool cacheBufferPool;
    private final Long2ObjectHashMap<Correlation> correlations;
    private final LongSupplier supplyCorrelationId;
    private final LongObjectBiConsumer<Runnable> scheduler;

    private final Writer writer;
    private final Long2ObjectHashMap<FanOut> junctions;


    private final Cache cache;

    // DPW to adjust
    private final ProxyCacheStreamFactory cacheStreamFactory;
    private final ConnectStreamFactory proxyConnectStreamFactory;

    public ProxyStreamFactory(
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<Correlation> correlations,
        LongObjectBiConsumer<Runnable> scheduler,
        Cache cache)
    {
        this.router = requireNonNull(router);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.streamBufferPool = requireNonNull(bufferPool);
        this.cacheBufferPool = bufferPool.duplicate();
        this.correlations = requireNonNull(correlations);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.scheduler = requireNonNull(scheduler);
        this.cache = cache;
        this.writer = new Writer(writeBuffer);
        this.junctions = new Long2ObjectHashMap<>();
        this.proxyConnectStreamFactory =
                new ConnectStreamFactory(
                    supplyCorrelationId,
                    supplyStreamId,
                    correlations,
                    writer,
                    router);
        this.cacheStreamFactory =
            new ProxyCacheStreamFactory(
                proxyConnectStreamFactory,
                supplyCorrelationId,
                scheduler,
                cacheBufferPool,
                correlations,
                supplyStreamId,
                writer);

    }

    @Override
    public MessageConsumer newStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length,
            MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
            final BeginFW begin,
            final MessageConsumer networkThrottle)
    {
        final long networkRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return networkRef == route.sourceRef() &&
                    acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long networkId = begin.streamId();

            newStream = new ProxyAcceptStream(networkThrottle, networkId)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
            final BeginFW begin,
            final MessageConsumer throttle)
    {
        final long throttleId = begin.streamId();

        return new ProxyConnectReplyStream(throttle, throttleId)::handleStream;
    }

    public final class ProxyAcceptStream
    {
        final MessageConsumer acceptThrottle;

        private String acceptName;
        long acceptCorrelationId;
        final long acceptStreamId;

//        private MessageConsumer acceptReply; // private
        String acceptReplyName;

//        private MessageConsumer connect; // private
        String connectName;
        long connectRef;
        long connectStreamId;

        private MessageConsumer streamState;

        private ProxyAcceptStream(
                MessageConsumer acceptThrottle,
                long acceptStreamId)
        {
            this.acceptThrottle = acceptThrottle;
            this.acceptStreamId = acceptStreamId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                this.acceptName = begin.source().asString();
                handleBegin(begin);
            }
            else
            {
                writer.doReset(acceptThrottle, acceptStreamId);
            }
        }

        private void handleBegin(BeginFW begin)
        {
            final long acceptRef = beginRO.sourceRef();
            final RouteFW connectRoute = resolveTarget(acceptRef, acceptName);

            if (connectRoute == null)
            {
                writer.doReset(acceptThrottle, acceptStreamId);
            }
            else
            {
                this.connectName = connectRoute.target().asString();
                final MessageConsumer connect = router.supplyTarget(connectName);
                this.connectRef = connectRoute.targetRef();

                final MessageConsumer acceptReply = router.supplyTarget(acceptName);
                this.acceptCorrelationId = begin.correlationId();

                final OctetsFW extension = beginRO.extension();
                final HttpBeginExFW httpBeginFW = extension.get(httpBeginExRO::wrap);
                final ListFW<HttpHeaderFW> requestHeaders = httpBeginFW.headers();

                if (requestShouldBypassCache(requestHeaders))
                {
                    this.streamState = proxyConnectStreamFactory.proxy(this, requestHeaders, connect, acceptReply);
                }
                else if (requestWantsResponseOnCacheUpdate(requestHeaders))
                {
                    cacheStreamFactory.subscribeToCacheUpdate(this, requestHeaders, acceptReply, connect);
                    this.streamState = this::responseInFlight;
                }
                else
                {
                    cacheStreamFactory.serviceRequest(this, requestHeaders, acceptReply, connect);
                    this.streamState = this::responseInFlight;
                }
            }
        }

        private void responseInFlight(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
            case EndFW.TYPE_ID:
                // NOOP
                break;
            case AbortFW.TYPE_ID:
            default:
                // The response is in flight and will be RESET
                // if there is a hard failure.
                break;
            }
        }

        void handleConnectThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleConnectWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleConnectReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleConnectWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();

            writer.doWindow(acceptThrottle, acceptStreamId, bytes, frames);
        }

        private void handleConnectReset(
            ResetFW reset)
        {
            writer.doReset(acceptThrottle, acceptStreamId);
        }

    }

    private final class ProxyConnectReplyStream
    {
        private MessageConsumer streamState;

        private final MessageConsumer connectReplyThrottle;
        private final long connectReplyStreamId;

        private Correlation streamCorrelation;
        private MessageConsumer acceptReply;

        // For initial caching
        private int cacheResponseSlot = NO_SLOT;
        private int cacheResponseSize = 0;
        private int cacheResponseHeadersSize = 0;

        // For trying to match existing cache
        private int cachedResponseSize;
        private int processedResponseSize;
        private CacheResponseServer cacheServer;

        private ProxyConnectReplyStream(
                MessageConsumer connectReplyThrottle,
                long connectReplyId)
        {
            this.connectReplyThrottle = connectReplyThrottle;
            this.connectReplyStreamId = connectReplyId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                writer.doReset(connectReplyThrottle, connectReplyStreamId);
            }
        }

        private void handleBegin(
                BeginFW begin)
        {
            final long connectReplyRef = begin.sourceRef();
            final long connectCorrelationId = begin.correlationId();

            this.streamCorrelation = connectReplyRef == 0L ?
                correlations.remove(connectCorrelationId) : null;

            if (streamCorrelation != null)
            {
                if (streamCorrelation.follow304())
                {
                    final OctetsFW extension = begin.extension();
                    final HttpBeginExFW httpBeginFW = extension.get(httpBeginExRO::wrap);
                    final ListFW<HttpHeaderFW> responseHeaders = httpBeginFW.headers();
                    final ListFW<HttpHeaderFW> requestHeaders = streamCorrelation.requestHeaders(pendingRequestHeadersRO);
                    if (requestHeaders.anyMatch(h -> X_POLL_INJECTED.equals(h.name().asString())) &&
                        responseHeaders.anyMatch(h -> STATUS.equals(h.name().asString())
                                            && "304".equals(h.value().asString())))
                    {
                        redoRequest(requestHeaders);
                    }
                    else
                    {
                        int requestURLHash = streamCorrelation.requestURLHash();
                        if(cache.hasStoredResponseThatSatisfies(requestURLHash, requestHeaders, true) != null)
                        {
                            this.cacheServer = cache.hasStoredResponseThatSatisfies(requestURLHash, requestHeaders, true);
                            forwardIfModified(begin, streamCorrelation, cacheServer, responseHeaders);
                        }
                        else
                        {
                            forwardBeginToAcceptReply(begin, streamCorrelation);
                        }
                    }
                }
                else
                {
                    forwardBeginToAcceptReply(begin, streamCorrelation);
                }
            }
            else
            {
                writer.doReset(connectReplyThrottle, connectReplyStreamId);
            }
        }

        private void redoRequest(
                final ListFW<HttpHeaderFW> requestHeaders)
        {
            final String connectName = streamCorrelation.connectName();
            final long connectRef = streamCorrelation.connectRef();
            final MessageConsumer newConnect = router.supplyTarget(connectName);
            final long newConnectStreamId = supplyStreamId.getAsLong();
            final long newConnectCorrelationId = supplyCorrelationId.getAsLong();

            String pollTime = getHeader(requestHeaders, "x-retry-after");
            if (pollTime != null)
            {
                scheduler.accept(currentTimeMillis() + parseInt(pollTime) * 1000, () ->
                    sendRequest(newConnect,
                                newConnectStreamId,
                                connectRef,
                                newConnectCorrelationId,
                                requestHeaders)
                );
            }
            else
            {
                sendRequest(newConnect, newConnectStreamId, connectRef, newConnectCorrelationId, requestHeaders);
            }
            correlations.put(newConnectCorrelationId, streamCorrelation);
            streamState = this::ignoreRest;
        }

        private void forwardBeginToAcceptReply(
                final BeginFW begin,
                final Correlation streamCorrelation)
        {

            streamCorrelation.connectReplyStreamId(connectReplyStreamId);
            acceptReply = streamCorrelation.consumer();

            final boolean requestCouldBeCached = streamCorrelation.requestSlot() != NO_SLOT;
            if (requestCouldBeCached && cache(begin))
            {
                streamState = this::cacheAndForwardBeginToAcceptReply;
                streamCorrelation.setConnectReplyThrottle((msgTypeId, buffer, index, length) ->
                {
                    switch(msgTypeId)
                    {
                        case ResetFW.TYPE_ID:
                            // DPW TODO stop caching!
                        default:
                            break;
                    }
                    this.connectReplyThrottle.accept(msgTypeId, buffer, index, length);
                });
                this.acceptReply.accept(BeginFW.TYPE_ID, begin.buffer(), begin.offset(), begin.sizeof());
            }
            else
            {
                streamState = acceptReply;
                streamCorrelation.setConnectReplyThrottle(connectReplyThrottle);
                acceptReply.accept(BeginFW.TYPE_ID, begin.buffer(), begin.offset(), begin.sizeof());
                if (requestCouldBeCached)
                {
                    streamCorrelation.cleanUp();
                }
            }

        }

        private void cacheAndForwardBeginToAcceptReply(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            acceptReply.accept(msgTypeId, buffer, index, length);
            boolean continueCaching = true;
                switch (msgTypeId)
                {
                    case DataFW.TYPE_ID:
                        DataFW data = dataRO.wrap(buffer, index, index + length);
                        continueCaching = cache(data);
                        break;
                    case EndFW.TYPE_ID:
                        EndFW end = endRO.wrap(buffer, index, index + length);
                        continueCaching = cache(end);
                        break;
                    case AbortFW.TYPE_ID:
                        AbortFW abort = abortRO.wrap(buffer, index, index + length);
                        continueCaching = cache(abort);
                        break;
                }
            if (!continueCaching)
            {
                streamCorrelation.cleanUp();
                streamState = this.acceptReply;
            }
        }

        private void ignoreRest(int msgTypeId, DirectBuffer buffer, int index, int length)
        {
            // NOOP
        }

        private void forwardIfModified(
                final BeginFW begin,
                final Correlation streamCorrelation,
                CacheResponseServer cacheServer,
                ListFW<HttpHeaderFW> responseHeaders)
        {
            ListFW<HttpHeaderFW> cachedResponseHeaders = cacheServer.getResponseHeaders();
            String cachedStatus = getHeader(cachedResponseHeaders, STATUS);
            String status = getHeader(responseHeaders, STATUS);
            String cachedContentLength = getHeader(cachedResponseHeaders, CONTENT_LENGTH);
            String contentLength = getHeader(cachedResponseHeaders, CONTENT_LENGTH);
            if (cachedStatus.equals(status) && Objects.equals(contentLength, cachedContentLength))
            {
                this.cacheServer = cacheServer;
                cacheServer.addClient();
                // store headers for in case if fails
                this.cacheResponseSlot = streamBufferPool.acquire(streamCorrelation.requestURLHash());
                cacheResponseHeaders(responseHeaders);

                this.cachedResponseSize = cacheServer.getResponse(octetsRO).sizeof();
                this.processedResponseSize = 0;
                final int bytes = cachedResponseSize + 8024;
                writer.doWindow(connectReplyThrottle, connectReplyStreamId, bytes, bytes);
                streamState = this::attemptCacheMatch;
            }
            else
            {
                forwardBeginToAcceptReply(begin, streamCorrelation);
            }
        }

        private void attemptCacheMatch(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    final OctetsFW payload = data.payload();
                    final int sizeofData = payload.sizeof();
                    final OctetsFW cachedPayload = cacheServer.getResponse(octetsRO);
                    boolean matches = (cachedResponseSize - processedResponseSize >= sizeofData) &&
                    confirmMatch(payload, cachedPayload, processedResponseSize);
                    if (!matches)
                    {
                        forwardHalfCachedResponse(payload);
                    }
                    else
                    {
                        this.processedResponseSize += sizeofData;
                    }
                    // consider removing window update when
                    // https://github.com/reaktivity/k3po-nukleus-ext.java/issues/16
                    break;
                case EndFW.TYPE_ID:
                    if (processedResponseSize == cachedResponseSize)
                    {
                        streamBufferPool.release(cacheResponseSlot);
                        final ListFW<HttpHeaderFW> requestHeaders = streamCorrelation.requestHeaders(pendingRequestHeadersRO);
                        redoRequest(requestHeaders);
                    }
                    else
                    {
                        forwardCompletelyCachedRespone(buffer, index, length);
                    }
                    break;
                case BeginFW.TYPE_ID:
                case AbortFW.TYPE_ID:
                default:
                    //error cases// Forward 503?
            }
        }

        private void forwardCompletelyCachedRespone(
                DirectBuffer buffer,
                int index,
                int length)
        {
            MutableDirectBuffer storeResponseBuffer = cacheBufferPool.buffer(cacheResponseSlot);
            storeResponseBuffer.putBytes(
                    this.cacheResponseSize,
                    cacheServer.getResponse(octetsRO).buffer(),
                    0,
                    processedResponseSize);
            this.cachedResponseSize += processedResponseSize;
            cache(endRO.wrap(buffer, index, index + length));
            int requstURLHash = streamCorrelation.requestURLHash();
            CacheResponseServer serverStream = cache.get(requstURLHash);
            serverStream.serveClient(streamCorrelation);
        }

        private boolean confirmMatch(
                OctetsFW payload,
                OctetsFW cachedPayload,
                int processedResponseSize)
        {
            final int payloadSizeOf = payload.sizeof();
            final int payloadOffset = payload.offset();
            final DirectBuffer payloadBuffer = payload.buffer();

            final int cachedPayloadSizeOf = cachedPayload.sizeof();
            final int cachedPayloadOffset = cachedPayload.offset();
            final DirectBuffer cachedPayloadBuffer = cachedPayload.buffer();

            if(payloadSizeOf <= cachedPayloadSizeOf)
            {
                for (int i = 0, length = payloadSizeOf; i < length; i++)
                {
                    if(cachedPayloadBuffer.getByte(cachedPayloadOffset + processedResponseSize + i)
                            != payloadBuffer.getByte(payloadOffset + i))
                    {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        private void forwardHalfCachedResponse(
                OctetsFW payload)
        {
            MutableDirectBuffer buffer = cacheBufferPool.buffer(cacheResponseSlot);

            // copy overwhat has been matched
            if(processedResponseSize > 0)
            {
                final OctetsFW cachedResponse = cacheServer.getResponse(octetsRO);
                final DirectBuffer cachedBuffer = cachedResponse.buffer();
                final int cachedOffset = octetsRO.offset();
                buffer.putBytes(this.cacheResponseSize, cachedBuffer, cachedOffset, processedResponseSize);
                this.cacheResponseSize += processedResponseSize;
            }

            // add new payload
            int payloadSize = payload.sizeof();
            buffer.putBytes(this.cacheResponseSize, payload.buffer(), payload.offset(), payloadSize);
            this.cacheResponseSize += payloadSize;

            streamState = this::waitForFullResponseThenForward;
        }

        private void waitForFullResponseThenForward(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            boolean cachedSuccessfully = true;
            switch(msgTypeId)
            {
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    cachedSuccessfully = cache(data);
                    break;
                case EndFW.TYPE_ID:
                    EndFW end = endRO.wrap(buffer, index, index + length);
                    cachedSuccessfully = cache(end);
                    if (cachedSuccessfully)
                    {
                        int requstURLHash = streamCorrelation.requestURLHash();
                        CacheResponseServer serverStream = cache.get(requstURLHash);
                        serverStream.serveClient(streamCorrelation);
                    }
                    break;
                case AbortFW.TYPE_ID:
                default:
                    // NOTE odd behavior if there is a cached Resource XYZ and then a
                    // new one comes in that matches but appends, i.e. XYZAB AND XYZAB
                    // size exceed cache capacity then it will 503...
                    sendAbortOnCacheOOM();
            }
            if (!cachedSuccessfully)
            {
                sendAbortOnCacheOOM();
            }
        }

        private void sendAbortOnCacheOOM()
        {
            long acceptReplyStreamId = supplyStreamId.getAsLong();
            writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, 0L, e ->
            e.item(h -> h.representation((byte) 0)
                    .name(":status")
                    .value("503")));
            writer.doAbort(acceptReply, acceptReplyStreamId);
        }

        private boolean cache(BeginFW begin)
        {
            final OctetsFW extension = begin.extension();
            final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::wrap);
            final ListFW<HttpHeaderFW> responseHeaders = httpBeginEx.headers();
            final boolean isCacheable = HttpCacheUtils.isPublicCacheableResponse(responseHeaders);
            final ListFW<HttpHeaderFW> requestHeaders = streamCorrelation.requestHeaders(pendingRequestHeadersRO);
            if (isCacheable && !requestHeaders.anyMatch(HttpCacheUtils::isCacheControlNoStore))
            {
                this.cacheResponseSlot = cacheBufferPool.acquire(this.connectReplyStreamId);
                if (cacheResponseSlot == NO_SLOT)
                {
                    return false;
                }
                int sizeof = responseHeaders.sizeof();
                if (cacheResponseSize  + sizeof > cacheBufferPool.slotCapacity())
                {
                    cacheBufferPool.release(this.cacheResponseSlot);
                    return false;
                }
                else
                {
                    cacheResponseHeaders(responseHeaders);
                    return true;
                }
            }
            else
            {
                return false;
            }
        }

        private void cacheResponseHeaders(final ListFW<HttpHeaderFW> responseHeaders)
        {
            MutableDirectBuffer buffer = cacheBufferPool.buffer(this.cacheResponseSlot);
            final int headersSize = responseHeaders.sizeof();
            buffer.putBytes(cacheResponseSize, responseHeaders.buffer(), responseHeaders.offset(), headersSize);
            cacheResponseSize += headersSize;
            this.cacheResponseHeadersSize = headersSize;
        }

        private boolean cache(DataFW data)
        {
            OctetsFW payload = data.payload();
            int sizeof = payload.sizeof();
            if (cacheResponseSize + sizeof + 4 > cacheBufferPool.slotCapacity())
            {
                cacheBufferPool.release(this.cacheResponseSlot);
                return false;
            }
            else
            {
                MutableDirectBuffer buffer = cacheBufferPool.buffer(this.cacheResponseSlot);
                buffer.putBytes(cacheResponseSize, payload.buffer(), payload.offset(), sizeof);
                cacheResponseSize += sizeof;
                return true;
            }
        }

        private boolean cache(EndFW end)
        {
            // TODO H2 end headers
            final int requestURLHash = this.streamCorrelation.requestURLHash();
            final int requestSlot = this.streamCorrelation.requestSlot();
            final int requestSize = this.streamCorrelation.requestSize();
            final int responseSlot = this.cacheResponseSlot;
            final int responseHeadersSize = this.cacheResponseHeadersSize;
            final int responseSize = this.cacheResponseSize;
            cache.put(requestURLHash, requestSlot, requestSize, responseSlot, responseHeadersSize, responseSize);
            return true;
        }

        private boolean cache(AbortFW abort)
        {
              cacheBufferPool.release(this.cacheResponseSlot);
              return false;
        }
    }

    private final class FanOut implements MessageConsumer
    {

        private final Set<MessagePredicate> outs;
        private Correlation streamCorrelation;
        private GroupThrottle connectReplyThrottle;

        FanOut()
        {
            this.outs = new HashSet<>();
        }

        public Set<MessagePredicate> getOuts()
        {
            return outs;
        }

        public MessageConsumer getHandleAcceptReplyThrottle()
        {
            return this::handleAcceptThrottle;
        }

        public void setStreamCorrelation(Correlation correlation)
        {
            this.streamCorrelation = correlation;
        }

        public Correlation getStreamCorrelation()
        {
            return this.streamCorrelation;
        }

        private void addSubscriber(MessagePredicate out)
        {
            outs.add(out);
        }

        private void unsubscribe(MessagePredicate out)
        {
            outs.remove(out);
        }

        @Override
        public void accept(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {

            for (Iterator<MessagePredicate> i = outs.iterator(); i.hasNext();)
            {
                MessagePredicate messageConsumer = i.next();
                if (!messageConsumer.test(msgTypeId, buffer, index, length))
                {
                    i.remove();
                }
                junctions.remove(this.streamCorrelation.requestURLHash());

            }
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final MessageConsumer connectReply = streamCorrelation.connectReplyThrottle();
                final long connectReplyStreamId = streamCorrelation.getConnectReplyStreamId();
                this.connectReplyThrottle = new GroupThrottle(outs.size(), writer, connectReply, connectReplyStreamId);
            }
        }

        public void handleAcceptThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleConnectWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleConnectReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleConnectWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();
            final long streamId = window.streamId();
            connectReplyThrottle.processWindow(streamId, bytes, frames);
        }

        private void handleConnectReset(
            ResetFW reset)
        {
            final long streamId = reset.streamId();
            connectReplyThrottle.processReset(streamId);
        }
    }

    RouteFW resolveTarget(
            long sourceRef,
            String sourceName)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = routeRO.wrap(b, o, l);
            return sourceRef == route.sourceRef() && sourceName.equals(route.source().asString());
        };

        return router.resolve(filter, this::wrapRoute);
    }

    private RouteFW wrapRoute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private void sendRequest(
            final MessageConsumer connect,
            final long connectStreamId,
            final long connectRef,
            final long connectCorrelationId,
            final ListFW<HttpHeaderFW> requestHeaders)
    {
        boolean stripNoCacheValue = false;

        Predicate<HttpHeaderFW> isInjected = h -> X_POLL_INJECTED.equals(h.name().asString())
                || "x-http-cache-sync".equals(h.name().asString());
        if (requestHeaders.anyMatch(INJECTED_HEADER_AND_NO_CACHE) && requestHeaders.anyMatch(NO_CACHE_CACHE_CONTROL))
            {
                isInjected = isInjected.or(h -> HttpHeaders.CACHE_CONTROL.equals(h.name().asString()));
                if (requestHeaders.anyMatch(h -> HttpHeaders.CACHE_CONTROL.equals(h.name().asString())))
                {
                    stripNoCacheValue = true;
                }
        }
        final Predicate<HttpHeaderFW> forwardHeader = isInjected.negate();
        final boolean stripNoCacheValue2 = stripNoCacheValue;

        writer.doHttpBegin2(
            connect,
            connectStreamId,
            connectRef,
            connectCorrelationId,
            hs -> requestHeaders.forEach(h ->
            {
                final StringFW nameRO = h.name();
                final String name = nameRO.asString();
                final String16FW valueRO = h.value();
                final String value = valueRO.asString();
                if (forwardHeader.test(h))
                {
                    hs.item(b ->
                        b.representation((byte) 0).name(nameRO).value(valueRO)
                    );
                }
                else if (stripNoCacheValue2 && HttpHeaders.CACHE_CONTROL.equals(name) && !"no-cache".equals(value))
                {
                    hs.item(b ->
                        {
                            String replaceFirst = value.replaceFirst(",?\\s*no-cache", "");
                            b.representation((byte) 0)
                                    .name(nameRO)
                                    .value(replaceFirst);
                        }
                    );
                }
        }));
        writer.doHttpEnd(connect, connectStreamId);
    }

    // TODO add to Long2ObjectHashMap#putIfAbsent.putIfAbsent without Boxing
    public static <T> T long2ObjectPutIfAbsent(
            Long2ObjectHashMap<T> map,
            int key,
            T value)
    {
        T old = map.get(key);
        if (old == null)
        {
            map.put(key, value);
        }
        return old;
    }
}

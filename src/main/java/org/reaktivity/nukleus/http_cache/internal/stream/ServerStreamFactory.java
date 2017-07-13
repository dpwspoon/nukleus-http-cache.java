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

import static java.util.Objects.requireNonNull;

import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http_cache.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_cache.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_cache.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteHandler;
import org.reaktivity.nukleus.stream.StreamFactory;

public class ServerStreamFactory implements StreamFactory
{

    private final BeginFW beginRO = new BeginFW();
    private final RouteFW routeRO = new RouteFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final RouteHandler router;

    private final LongSupplier supplyStreamId;
    private final Writer writer;

    public ServerStreamFactory(
        RouteHandler router,
        MutableDirectBuffer writeBuffer,
        LongSupplier supplyStreamId)
    {
        this.router = requireNonNull(router);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.writer = new Writer(writeBuffer);
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

        final MessageConsumer newStream = newAcceptStream(begin, throttle);

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

    private final class ProxyAcceptStream
    {
        private final MessageConsumer acceptThrottle;
        private final long acceptStreamId;

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
                handleBegin(begin);
            }
            else
            {
                writer.doReset(acceptThrottle, acceptStreamId);
            }
        }

        private void handleBegin(
                BeginFW begin)
        {
            final long acceptRef = beginRO.sourceRef();
            final String acceptName = begin.source().asString();
            final RouteFW connectRoute = resolveTarget(acceptRef, acceptName);

            if (connectRoute == null)
            {
                writer.doReset(acceptThrottle, acceptStreamId);
            }
            else
            {
                final MessageConsumer acceptReply = router.supplyTarget(acceptName);
                final long acceptReplyStreamId =  supplyStreamId.getAsLong();
                final long acceptCorrelationId = begin.correlationId();

                writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, hs ->
                {
                    hs.item(h -> h.name(":status").value("200"));
                    hs.item(h -> h.name("content-type").value("text/event-stream"));
                });
                writer.doWindow(acceptThrottle, acceptStreamId, 0, 0);
                this.streamState = this::afterBegin;
                router.setThrottle(acceptName, acceptReplyStreamId, this::handleThrottle);
            }
        }

        private void afterBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
            case EndFW.TYPE_ID:
                break;

//          TODO
//            case AbortFW.TYPE_ID:
//                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
//                handleAbort(abort);
//                break;
            case DataFW.TYPE_ID:
            default:
                writer.doReset(acceptThrottle, acceptStreamId);
                break;
            }
        }

        private void handleThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleWindow(
            WindowFW window)
        {
            // NOOP, TODO this means we can send data,
            // but (for now) we don't want to...
        }

        private void handleReset(
            ResetFW reset)
        {
            writer.doReset(acceptThrottle, acceptStreamId);
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
}

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
package org.reaktivity.nukleus.http_cache.internal.stream.util;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableList;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.CacheDirectives.MAX_AGE;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.CacheDirectives.NO_CACHE;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.CacheDirectives.NO_STORE;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CACHE_CONTROL;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.CONTENT_LENGTH;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.METHOD;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.STATUS;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.TRANSFER_ENCODING;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.X_HTTP_CACHE_SYNC;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeaders.X_POLL_INJECTED;
import static org.reaktivity.nukleus.http_cache.internal.stream.util.HttpHeadersUtil.getHeader;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.agrona.LangUtil;
import org.reaktivity.nukleus.http_cache.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_cache.internal.types.ListFW;

public final class HttpCacheUtils
{

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");

    public static final List<String> CACHEABLE_BY_DEFAULT_STATUS_CODES = unmodifiableList(
            asList("200", "203", "204", "206", "300", "301", "404", "405", "410", "414", "501"));

    private HttpCacheUtils()
    {
        // utility class
    }

    public static boolean requestShouldBypassCache(
        ListFW<HttpHeaderFW> headers)
    {
        return !headers.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            switch (name)
            {
                case CACHE_CONTROL:
                    return value.contains("no-cache");
                case METHOD:
                    return !"GET".equalsIgnoreCase(value);
                case CONTENT_LENGTH:
                    return true;
                case TRANSFER_ENCODING:
                    return true;
                default:
                    return false;
                }
        });
    }

    public static boolean requestWantsResponseOnCacheUpdate(final ListFW<HttpHeaderFW> requestHeaders)
    {
        return !requestHeaders.anyMatch(
                h ->
                {
                    String name = h.name().asString();
                    return  X_POLL_INJECTED.equals(name) ||
                            X_HTTP_CACHE_SYNC.equals(name);
                });
    }

    public static boolean canInjectPushPromise(
            ListFW<HttpHeaderFW> headers)
    {
        return !headers.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            switch (name)
            {
            case METHOD:
                return !"GET".equalsIgnoreCase(value);
            case CONTENT_LENGTH:
                return true;
            default:
                return false;
            }
        });
    }

    public static boolean cachedResponseCanSatisfyRequest(
            final ListFW<HttpHeaderFW> cachedRequestHeaders,
            final ListFW<HttpHeaderFW> cachedResponseHeaders,
            final ListFW<HttpHeaderFW> requestHeaders)
    {

        final String cachedVaryHeader = getHeader(cachedResponseHeaders, "vary");
        final String cachedAuthorizationHeader = getHeader(cachedRequestHeaders, "authorization");
        final String cachedCacheControlHeader = getHeader(cachedResponseHeaders, CACHE_CONTROL);

        final String requestAuthorizationHeader = getHeader(requestHeaders, "authorization");
        final String requestCacheControlHeader = getHeader(requestHeaders, CACHE_CONTROL);

        if (requestCacheControlHeader != null)
        {
            if(!responseSatisfiesRequestDirectives(cachedResponseHeaders, requestCacheControlHeader))
            {
                return false;
            }
        }

        boolean useSharedResponse = true;

        if (cachedCacheControlHeader != null && cachedCacheControlHeader.contains("public"))
        {
            useSharedResponse = true;
        }
        else if (cachedCacheControlHeader != null && cachedCacheControlHeader.contains("private"))
        {
            useSharedResponse = false;
        }
        else if (requestAuthorizationHeader != null || cachedAuthorizationHeader != null)
        {
            useSharedResponse = false;
        }
        else if (cachedVaryHeader != null)
        {
            useSharedResponse = stream(cachedVaryHeader.split("\\s*,\\s*")).anyMatch(v ->
            {
                String pendingHeaderValue = getHeader(cachedRequestHeaders, v);
                String myHeaderValue = getHeader(requestHeaders, v);
                return Objects.equals(pendingHeaderValue, myHeaderValue);
            });
        }

        return useSharedResponse;
    }

    private static boolean responseSatisfiesRequestDirectives(
            final ListFW<HttpHeaderFW> responseHeaders,
            final String myRequestCacheControl)
    {
        // TODO in future, clean up GC/Object creation

        // Check max-age=0;
        HttpCacheUtils.CacheControlParser parsedCacheControl = new HttpCacheUtils.CacheControlParser(myRequestCacheControl);
        String requestMaxAge = parsedCacheControl.getValue(MAX_AGE);
        if(requestMaxAge != null)
        {
            String dateHeader = getHeader(responseHeaders, "date");
            if (dateHeader == null)
            {
                dateHeader = getHeader(responseHeaders, "last-modified");
            }
            if (dateHeader == null)
            {
                // invalid response, so say no
                return false;
            }
            try
            {
                Date receivedDate = DATE_FORMAT.parse(dateHeader);
                final int timeWhenExpires = Integer.parseInt(requestMaxAge) * 1000;
                if(new Date(System.currentTimeMillis() - timeWhenExpires).after(receivedDate))
                {
                    return false;
                }
            }
            catch(Exception e)
            {
                // Should never get here;
                LangUtil.rethrowUnchecked(e);
            }
        }
        return true;
    }

    public static boolean isExpired(ListFW<HttpHeaderFW> responseHeaders)
    {
        String dateHeader = getHeader(responseHeaders, "date");
        if (dateHeader == null)
        {
            dateHeader = getHeader(responseHeaders, "last-modified");
        }
        if (dateHeader == null)
        {
            // invalid response, so say it is expired
            return true;
        }
        try
        {
            Date receivedDate = DATE_FORMAT.parse(dateHeader);
            String cacheControl = HttpHeadersUtil.getHeader(responseHeaders, CACHE_CONTROL);
            String ageExpires = null;
            if (cacheControl != null)
            {
                CacheControlParser parsedCacheControl = new CacheControlParser(cacheControl);
                ageExpires = parsedCacheControl.getValue("s-maxage");
                if (ageExpires == null)
                {
                    ageExpires = parsedCacheControl.getValue("max-age");
                }
            }
            int ageExpiresInt;
            if (ageExpires == null)
            {
                String lastModified = getHeader(responseHeaders, "last-modified");
                if (lastModified == null)
                {
                    ageExpiresInt = 5000; // default to 5
                }
                else
                {
                    Date lastModifiedDate = DATE_FORMAT.parse(lastModified);
                    ageExpiresInt = (int) ((receivedDate.getTime() - lastModifiedDate.getTime()) * (10.0f/100.0f));
                }
            }
            else
            {
                ageExpiresInt = Integer.parseInt(ageExpires) * 1000;
            }
            final Date expires = new Date(System.currentTimeMillis() - ageExpiresInt);
            return expires.after(receivedDate);
        }
        catch (Exception e)
        {
            // Error so just expire it
            return true;
        }
    }

    // Apache Version 2.0 (July 25, 2017)
    // https://svn.apache.org/repos/asf/abdera/java/trunk/
    // core/src/main/java/org/apache/abdera/protocol/util/CacheControlUtil.java
    // TODO GC free
    public static class CacheControlParser implements Iterable<String>
    {

        private static final String REGEX =
            "\\s*([\\w\\-]+)\\s*(=)?\\s*(\\d+|\\\"([^\"\\\\]*(\\\\.[^\"\\\\]*)*)+\\\")?\\s*";

        private static final Pattern CACHE_DIRECTIVES = Pattern.compile(REGEX);

        private HashMap<String, String> values = new HashMap<>();

        public CacheControlParser(String value)
        {
            values.clear();
            Matcher matcher = CACHE_DIRECTIVES.matcher(value);
            while (matcher.find())
            {
                String directive = matcher.group(1);
                values.put(directive, matcher.group(3));
            }
        }

        public Iterator<String> iterator()
        {
            return values.keySet().iterator();
        }

        public Map<String, String> getValues()
        {
            return values;
        }

        public String getValue(String directive)
        {
            return values.get(directive);
        }

        public List<String> getValues(String directive)
        {
            String dValues = getValue(directive);
            if (dValues != null)
            {
                return Arrays
                        .stream(dValues.split(","))
                        .map(String::trim)
                        .collect(Collectors.toList());
            }
            return null;
        }

    }

    public static boolean isPublicCacheableResponse(ListFW<HttpHeaderFW> responseHeaders)
    {
        if (responseHeaders.anyMatch(h ->
                CACHE_CONTROL.equals(h.name().asString())
                && h.value().asString().contains("private")))
        {
            return false;
        }
        return isPrivateCacheableResponse(responseHeaders);
    }

    public static boolean isPrivateCacheableResponse(ListFW<HttpHeaderFW> responseHeaders)
    {
        String cacheControl = getHeader(responseHeaders, "cache-control");
        if (cacheControl != null)
        {
            CacheControlParser parser = new  CacheControlParser(cacheControl);
            Iterator<String> iter = parser.iterator();
            while(iter.hasNext())
            {
                String directive = iter.next();
                switch(directive)
                {
                    // TODO expires
                    case NO_CACHE:
                        return false;
                    case CacheDirectives.PUBLIC:
                        return true;
                    case CacheDirectives.MAX_AGE:
                        return true;
                    case CacheDirectives.S_MAXAGE:
                        return true;
                    default:
                        break;
                }
            }
        }
        return responseHeaders.anyMatch(h ->
        {
            final String name = h.name().asString();
            final String value = h.value().asString();
            if (STATUS.equals(name))
            {
                return CACHEABLE_BY_DEFAULT_STATUS_CODES.contains(value);
            }
            return false;
        });
    }

    public static boolean isCacheControlNoStore(HttpHeaderFW header)
    {
        final String name = header.name().asString();
        final String value = header.value().asString();
        return HttpHeaders.CACHE_CONTROL.equals(name) && value.contains(NO_STORE);
    }
}

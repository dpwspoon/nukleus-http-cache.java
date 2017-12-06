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
package org.reaktivity.nukleus.http_cache.internal.test;

import static org.junit.Assert.assertEquals;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.nukleus.http_cache.internal.HttpCacheController;
import org.reaktivity.reaktor.test.ReaktorRule;

public class HttpCacheCountersRule implements TestRule
{
    private static final int NUM_OF_SLOTS_PER_CACHE_ENTRY = 3;
    private final ReaktorRule reaktor;

    public HttpCacheCountersRule(ReaktorRule reaktor)
    {
        this.reaktor = reaktor;
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        return new Statement()
        {

            @Override
            public void evaluate() throws Throwable
            {
                base.evaluate();
            }

        };
    }

    public long slabAquires()
    {
        return controller().count("entry.acquires");
    }

    public long slabReleases()
    {
        return controller().count("entry.releases");
    }

    public long cacheHits()
    {
        return controller().count("cache.hits");
    }

    public long cacheMisses()
    {
        return controller().count("cache.misses");
    }

    private HttpCacheController controller()
    {
        return reaktor.controller(HttpCacheController.class);
    }

    public void assertCacheHits(
            int expected)
    {
        assertEquals(expected, cacheHits());
    }

    public void assertCacheMisses(
            int expected)
    {
        assertEquals(expected, cacheMisses());
    }

    public void assertExpectedCacheEntries(
            int numberOfResponses)
    {
        assertEquals(NUM_OF_SLOTS_PER_CACHE_ENTRY * numberOfResponses, slabAquires() - slabReleases());
    }

    public void assertExpectedCacheEntries(
            int numberOfResponses,
            int cacheInitiatedRefreshes)
    {
        assertEquals(NUM_OF_SLOTS_PER_CACHE_ENTRY * numberOfResponses + cacheInitiatedRefreshes, slabAquires() - slabReleases());
    }

    public void assertExpectedCacheEntries(
            int numberOfResponses,
            int cacheInitiatedRefreshes,
            int requestPendingCacheUpdate)
    {
        assertEquals(
            NUM_OF_SLOTS_PER_CACHE_ENTRY * numberOfResponses + cacheInitiatedRefreshes + requestPendingCacheUpdate,
            slabAquires() - slabReleases());
    }
}

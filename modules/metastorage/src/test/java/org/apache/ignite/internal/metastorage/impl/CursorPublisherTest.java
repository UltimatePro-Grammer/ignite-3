/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metastorage.impl;

import static java.util.Collections.nCopies;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willFailFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willFailIn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.cursor.CloseCursorCommand;
import org.apache.ignite.internal.metastorage.command.cursor.CreateRangeCursorCommand;
import org.apache.ignite.internal.metastorage.command.cursor.NextBatchCommand;
import org.apache.ignite.internal.metastorage.command.response.BatchResponse;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.flow.TestFlowUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.NodeStoppingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link CursorPublisher} and {@link CursorSubscription}.
 */
@ExtendWith(MockitoExtension.class)
public class CursorPublisherTest {
    @Mock
    private RaftGroupService raftService;

    private MetaStorageServiceContext context;

    private CursorPublisher publisher;

    @BeforeEach
    void setUp() {
        context = new MetaStorageServiceContext(
                raftService,
                new MetaStorageCommandsFactory(),
                ForkJoinPool.commonPool(),
                new IgniteSpinBusyLock()
        );

        publisher = new CursorPublisher(context, uuid -> mock(CreateRangeCursorCommand.class));
    }

    @Test
    void testPagination() {
        Entry mockEntry = mock(Entry.class);

        when(raftService.run(any(NextBatchCommand.class)))
                .thenReturn(completedFuture(new BatchResponse(nCopies(6, mockEntry), true)))
                .thenReturn(completedFuture(new BatchResponse(nCopies(5, mockEntry), false)));

        when(raftService.run(any(CreateRangeCursorCommand.class))).thenReturn(completedFuture(null));

        var awaitFuture = new CompletableFuture<Void>();

        Subscriber<Entry> subscriber = mock(Subscriber.class);

        doAnswer(invocation -> {
            Subscription subscription = invocation.getArgument(0);

            subscription.request(Long.MAX_VALUE);

            return null;
        }).when(subscriber).onSubscribe(any());

        doAnswer(invocation -> awaitFuture.complete(null)).when(subscriber).onComplete();

        publisher.subscribe(subscriber);

        assertThat(awaitFuture, willCompleteSuccessfully());

        verify(raftService, times(2)).run(any(NextBatchCommand.class));
        verify(subscriber, times(11)).onNext(any());
        verify(subscriber).onComplete();
    }

    @Test
    void testRequestDuringOnNext() {
        Entry mockEntry = mock(Entry.class);

        when(raftService.run(any(NextBatchCommand.class)))
                .thenReturn(completedFuture(new BatchResponse(nCopies(6, mockEntry), true)))
                .thenReturn(completedFuture(new BatchResponse(nCopies(5, mockEntry), false)));

        when(raftService.run(any(CreateRangeCursorCommand.class))).thenReturn(completedFuture(null));

        CompletableFuture<List<Entry>> future = TestFlowUtils.subscribeToList(publisher);

        assertThat(future, will(hasSize(11)));

        verify(raftService, times(2)).run(any(NextBatchCommand.class));
    }

    @Test
    void testErrorOnCursorCreation() {
        when(raftService.run(any(CreateRangeCursorCommand.class))).thenReturn(failedFuture(new IllegalStateException()));

        var awaitFuture = new CompletableFuture<Void>();

        Subscriber<Entry> subscriber = mock(Subscriber.class);

        doAnswer(invocation -> {
            Throwable e = invocation.getArgument(0);

            awaitFuture.completeExceptionally(e);

            return null;
        }).when(subscriber).onError(any());

        publisher.subscribe(subscriber);

        assertThat(awaitFuture, willFailFast(IllegalStateException.class));
    }

    @Test
    void testErrorOnPagination() {
        when(raftService.run(any(NextBatchCommand.class)))
                .thenReturn(completedFuture(new BatchResponse(nCopies(5, mock(Entry.class)), true)))
                .thenReturn(failedFuture(new IllegalStateException()));

        when(raftService.run(any(CreateRangeCursorCommand.class))).thenReturn(completedFuture(null));
        when(raftService.run(any(CloseCursorCommand.class))).thenReturn(completedFuture(null));

        var awaitFuture = new CompletableFuture<Void>();

        Subscriber<Entry> subscriber = mock(Subscriber.class);

        doAnswer(invocation -> {
            Subscription subscription = invocation.getArgument(0);

            subscription.request(Long.MAX_VALUE);

            return null;
        }).when(subscriber).onSubscribe(any());

        doAnswer(invocation -> {
            Throwable e = invocation.getArgument(0);

            awaitFuture.completeExceptionally(e);

            return null;
        }).when(subscriber).onError(any());

        publisher.subscribe(subscriber);

        assertThat(awaitFuture, willFailIn(10, TimeUnit.SECONDS, IllegalStateException.class));

        verify(raftService, times(2)).run(any(NextBatchCommand.class));
        verify(subscriber, times(5)).onNext(any());
        verify(raftService).run(any(CloseCursorCommand.class));
    }

    @Test
    void testComponentStopOnSubscribe() {
        context.busyLock().block();

        Subscriber<Entry> subscriber = mock(Subscriber.class);

        publisher.subscribe(subscriber);

        verify(subscriber).onError(any(NodeStoppingException.class));
    }

    @Test
    void testComponentStopOnNext() throws Exception {
        when(raftService.run(any(CreateRangeCursorCommand.class))).thenReturn(completedFuture(null));
        when(raftService.run(any(CloseCursorCommand.class))).thenReturn(completedFuture(null));

        Subscriber<Entry> subscriber = mock(Subscriber.class);

        doAnswer(invocation -> {
            Subscription subscription = invocation.getArgument(0);

            subscription.request(Long.MAX_VALUE);

            return null;
        }).when(subscriber).onSubscribe(any());

        // Barrier used to wait when the first page of data is retrieved.
        var barrierBeforeLock = new CyclicBarrier(2);

        // Barrier used to wait for the busy lock to be blocked.
        var barrierAfterLock = new CyclicBarrier(2);

        Entry mockEntry = mock(Entry.class);

        when(raftService.run(any(NextBatchCommand.class)))
                .thenReturn(completedFuture(new BatchResponse(List.of(mockEntry), true)))
                .thenAnswer(invocation -> supplyAsync(() -> {
                    try {
                        barrierAfterLock.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                        throw new CompletionException(e);
                    }

                    return new BatchResponse(List.of(mockEntry), false);
                }));

        doAnswer(invocation -> {
            barrierBeforeLock.await(10, TimeUnit.SECONDS);

            return null;
        }).when(subscriber).onNext(any());

        publisher.subscribe(subscriber);

        barrierBeforeLock.await(10, TimeUnit.SECONDS);

        context.busyLock().block();

        barrierAfterLock.await(10, TimeUnit.SECONDS);

        verify(subscriber).onNext(any());
        verify(subscriber, timeout(10_000)).onError(any(NodeStoppingException.class));
    }

    @Test
    void testSingleOnlySubscription() {
        when(raftService.run(any(CreateRangeCursorCommand.class))).thenReturn(completedFuture(null));

        Subscriber<Entry> subscriber = mock(Subscriber.class);

        publisher.subscribe(subscriber);

        assertThrows(IllegalArgumentException.class, () -> publisher.subscribe(subscriber));
    }
}

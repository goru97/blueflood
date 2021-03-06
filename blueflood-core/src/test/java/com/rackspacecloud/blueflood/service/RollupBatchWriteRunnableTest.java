package com.rackspacecloud.blueflood.service;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.io.astyanax.AstyanaxWriter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

public class RollupBatchWriteRunnableTest {

    ArrayList<SingleRollupWriteContext> wcs;
    RollupExecutionContext ctx;
    AstyanaxWriter writer;
    RollupBatchWriteRunnable rbwr;

    @Before
    public void setUp() {
        wcs = new ArrayList<SingleRollupWriteContext>();
        ctx = mock(RollupExecutionContext.class);
        writer = mock(AstyanaxWriter.class);
        rbwr = new RollupBatchWriteRunnable(wcs, ctx, writer);
    }

    @Test
    public void runSendsRollupsToWriterAndDecrementsCount() throws ConnectionException {

        // given
        final AtomicLong decrementCount = new AtomicLong(0);
        Answer contextAnswer = new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                decrementCount.set((Long)invocation.getArguments()[0]);
                return null;
            }
        };
        doAnswer(contextAnswer).when(ctx).decrementWriteCounter(anyLong());


        final Object[] insertRollupsArg = new Object[1];
        Answer writerAnswer = new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                insertRollupsArg[0] = invocation.getArguments()[0];
                return null;
            }
        };
        doAnswer(writerAnswer).when(writer).insertRollups(
                Matchers.<ArrayList<SingleRollupWriteContext>>any());

        // when
        rbwr.run();

        // then
        verify(writer).insertRollups(Matchers.<ArrayList<SingleRollupWriteContext>>any());
        assertSame(wcs, insertRollupsArg[0]);
        verifyNoMoreInteractions(writer);
        verify(ctx).decrementWriteCounter(anyLong());
        assertEquals(wcs.size(), decrementCount.get());
        verifyNoMoreInteractions(ctx);
    }

    @Test
    public void connectionExceptionMarksUnsuccessful() throws ConnectionException {

        // given
        Throwable cause = new ConnectionException("exception for testing purposes") { };
        doThrow(cause).when(writer).insertRollups(
                Matchers.<ArrayList<SingleRollupWriteContext>>any());

        // when
        rbwr.run();

        // then
        verify(writer).insertRollups(Matchers.<ArrayList<SingleRollupWriteContext>>any());
        verifyNoMoreInteractions(writer);
        verify(ctx).markUnsuccessful(Matchers.<Throwable>any());
        verify(ctx).decrementWriteCounter(anyLong());
        verifyNoMoreInteractions(ctx);
    }

    @Test
    public void otherExceptionBreaksEverything() throws ConnectionException {

        // given
        Throwable cause = new UnsupportedOperationException("exception for testing purposes");
        doThrow(cause).when(writer).insertRollups(
                Matchers.<ArrayList<SingleRollupWriteContext>>any());

        // when
        Throwable caught = null;
        try {
            rbwr.run();
        } catch (Throwable t) {
            caught = t;
        }

        // then
        assertNotNull(caught);
        assertSame(cause, caught);
        verify(writer).insertRollups(Matchers.<ArrayList<SingleRollupWriteContext>>any());
        verifyNoMoreInteractions(writer);
        verifyZeroInteractions(ctx);
    }
}

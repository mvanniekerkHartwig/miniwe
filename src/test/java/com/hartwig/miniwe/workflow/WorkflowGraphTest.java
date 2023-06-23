package com.hartwig.miniwe.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import com.hartwig.miniwe.miniwdl.DefinitionReader;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.ImmutableExecutionDefinition;
import com.hartwig.miniwe.miniwdl.ImmutableWorkflowDefinition;
import com.hartwig.miniwe.miniwdl.Stage;
import com.hartwig.miniwe.miniwdl.WorkflowDefinition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(5)
class WorkflowGraphTest {

    private ImmutableWorkflowDefinition simpleWorkflow;
    private ImmutableExecutionDefinition simpleExecution;
    private WorkflowDefinition concurrentWorkflow;
    private WorkflowDefinition veryConcurrentWorkflow;
    private WorkflowDefinition linearWorkflow;

    @BeforeEach
    void setUp() throws IOException {
        simpleWorkflow = WorkflowDefinition.builder()
                .name("wf")
                .version("1.0.0")
                .addStages(Stage.builder().name("simple-stage").image("eu.gcr.io/hmf-build/image").version("1.0.0").build())
                .build();
        simpleExecution = ExecutionDefinition.builder().name("ex").workflow("wf").version("1.0.0").build();
        concurrentWorkflow =
                new DefinitionReader().readWorkflow(getClass().getClassLoader().getResourceAsStream("concurrent-stage-workflow.yaml"));
        veryConcurrentWorkflow =
                new DefinitionReader().readWorkflow(getClass().getClassLoader().getResourceAsStream("very-concurrent-stage-workflow.yaml"));
        linearWorkflow = new DefinitionReader().readWorkflow(getClass().getClassLoader().getResourceAsStream("linear-stage-workflow.yaml"));
    }

    @Test
    void workflowNameShouldEqualExecutionName() {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var e = assertThrows(IllegalArgumentException.class,
                () -> workflowGraph.getOrCreateRun(mock(StageScheduler.class), Set.of(), simpleExecution.withWorkflow("other-workflow")));
        assertEquals("Workflow name 'wf-1-0-0' should be the same as execution name, but was 'other-workflow-1-0-0'", e.getMessage());
    }

    @Test
    void testSimpleWorkflowSucceeds() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertEquals(true, result.get());
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.SUCCESS), run.getStageStateView());
    }

    @Test
    void testSimpleWorkflowSucceedsSubscription() throws ExecutionException, InterruptedException {
        var executorService = ForkJoinPool.commonPool();
        var workflowGraph = new WorkflowGraph(simpleWorkflow, executorService);

        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenAnswer(stage -> CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        run.subscribe(stageStates::add);

        assertTrue(run.findOrStart().get());
        assertEquals(stageStates.size(), 3);
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.WAITING), stageStates.get(0));
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.RUNNING), stageStates.get(1));
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.SUCCESS), stageStates.get(2));
    }

    @Test
    void testSimpleWorkflowSucceedsGraph() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertEquals(true, result.get());
        assertEquals("strict digraph G {\n" + "  1 [ label=\"simple-stage\" color=\"green\" ];\n" + "}\n", run.toDotFormat());
    }

    @Test
    void testSimpleWorkflowFails() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(false));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertEquals(false, result.get());
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.FAILED), run.getStageStateView());
    }

    @Test
    void testSimpleWorkflowFailsSubscription() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());

        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenAnswer(stage -> CompletableFuture.completedFuture(false));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        run.subscribe(stageStates::add);

        assertFalse(run.findOrStart().get());
        assertEquals(stageStates.size(), 3);
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.WAITING), stageStates.get(0));
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.RUNNING), stageStates.get(1));
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.FAILED), stageStates.get(2));
    }

    @Test
    void testSimpleWorkflowFailsGraph() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(false));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertEquals(false, result.get());
        assertEquals("strict digraph G {\n" + "  1 [ label=\"simple-stage\" color=\"red\" ];\n" + "}\n", run.toDotFormat());
    }

    @Test
    void sameExecutionNameReturnsSameObject() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution).findOrStart().get();
        var sameRun = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.SUCCESS), sameRun.getStageStateView());
    }

    @Test
    void sameExecutionAfterDeleteReturnsDifferentObject() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution).findOrStart().get();
        workflowGraph.delete(simpleExecution);
        var newRun = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.WAITING), newRun.getStageStateView());
    }

    @Test
    void hangingStageCanBeCancelled() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());

        var stageScheduler = mock(StageScheduler.class);
        doAnswer(b -> CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(5_000);
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        })).when(stageScheduler).schedule(any());

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var success = run.findOrStart();
        run.cancel();
        assertFalse(success.get());
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.IGNORED), run.getStageStateView());
    }

    @Test
    void activeRunCanBeDeleted() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());

        var stageScheduler = mock(StageScheduler.class);
        doAnswer(b -> CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(5_000);
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        })).when(stageScheduler).schedule(any());

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var success = run.findOrStart();
        workflowGraph.delete(simpleExecution);
        assertFalse(success.get());
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.IGNORED), run.getStageStateView());
    }

    @Test
    void runThatDoesNotExistCannotBeDeleted() {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var e = assertThrows(IllegalArgumentException.class, () -> workflowGraph.delete(simpleExecution));
        assertEquals("Could not find execution with run name 'wf-1-0-0-ex' to delete.", e.getMessage());
    }

    @Test
    void cancellingRunBeforeStartingFails() {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var e = assertThrows(IllegalStateException.class, run::cancel);
        assertEquals("Cannot cancel run that was not started yet.", e.getMessage());
    }

    @Test
    void cachedStageIsSuccess() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of("simple-stage"), simpleExecution);
        assertTrue(run.findOrStart().get());
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.SUCCESS), run.getStageStateView());
        verifyNoMoreInteractions(stageScheduler);
    }

    @Test
    void startingRunTwiceReturnsAlreadyStartedRun() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);

        run.findOrStart().get();
        clearInvocations(stageScheduler);
        verifyNoMoreInteractions(stageScheduler);
        run.findOrStart().get();
    }

    @Test
    void twoRunsAreIndependent() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));
        var otherScheduler = mock(StageScheduler.class);
        when(otherScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(false));

        var run1 = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution).findOrStart();
        var run2 = workflowGraph.getOrCreateRun(otherScheduler, Set.of(), simpleExecution.withName("other-execution")).findOrStart();
        assertFalse(run2.get());
        assertTrue(run1.get());
    }

    @Test
    void testConcurrentWorkflowSucceeds() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(concurrentWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertEquals(true, result.get());
        var expected = Map.of("stage-b", WorkflowGraph.StageRunningState.SUCCESS, "stage-a", WorkflowGraph.StageRunningState.SUCCESS);
        assertEquals(expected, run.getStageStateView());
    }

    @Test
    void testConcurrentWorkflowOneFails() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(concurrentWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        doReturn(CompletableFuture.completedFuture(true)).when(stageScheduler)
                .schedule(argThat(stage -> stage.stage().name().equals("stage-a")));
        doReturn(CompletableFuture.completedFuture(false)).when(stageScheduler)
                .schedule(argThat(stage -> stage.stage().name().equals("stage-b")));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertEquals(false, result.get());
        var expected = Map.of("stage-b", WorkflowGraph.StageRunningState.FAILED, "stage-a", WorkflowGraph.StageRunningState.SUCCESS);
        assertEquals(expected, run.getStageStateView());
    }

    @Test
    void testVeryConcurrentWorkflowExecuteInOrder() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(veryConcurrentWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        var la = new CountDownLatch(1);
        var lb = new CountDownLatch(1);
        var lc = new CountDownLatch(1);

        doAnswer(a -> CompletableFuture.supplyAsync(() -> {
            la.countDown();
            return true;
        })).when(stageScheduler).schedule(argThat(stage -> stage.stage().name().equals("stage-a")));
        final var sleepTime = 50;
        doAnswer(b -> CompletableFuture.supplyAsync(() -> {
            try {
                la.await(10, TimeUnit.SECONDS);
                Thread.sleep(sleepTime);
                lb.countDown();
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        })).when(stageScheduler).schedule(argThat(stage -> stage.stage().name().equals("stage-b")));
        doAnswer(c -> CompletableFuture.supplyAsync(() -> {
            try {
                lb.await(10, TimeUnit.SECONDS);
                Thread.sleep(sleepTime);
                lc.countDown();
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        })).when(stageScheduler).schedule(argThat(stage -> stage.stage().name().equals("stage-c")));
        doAnswer(d -> CompletableFuture.supplyAsync(() -> {
            try {
                lc.await(10, TimeUnit.SECONDS);
                Thread.sleep(sleepTime);
                return true;
            } catch (InterruptedException e) {
                return false;
            }
        })).when(stageScheduler).schedule(argThat(stage -> stage.stage().name().equals("stage-d")));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();
        run.subscribe(stageStates::add);

        var result = run.findOrStart();
        assertEquals(true, result.get());

        assertEquals(6, stageStates.size());
        assertTrue(stageStates.get(0).values().stream().allMatch(state -> state == WorkflowGraph.StageRunningState.WAITING));
        assertTrue(stageStates.get(1).values().stream().allMatch(state -> state == WorkflowGraph.StageRunningState.RUNNING));
        assertEquals(WorkflowGraph.StageRunningState.SUCCESS, stageStates.get(2).get("stage-a"));
        assertEquals(WorkflowGraph.StageRunningState.SUCCESS, stageStates.get(3).get("stage-b"));
        assertEquals(WorkflowGraph.StageRunningState.SUCCESS, stageStates.get(4).get("stage-c"));
        assertEquals(WorkflowGraph.StageRunningState.SUCCESS, stageStates.get(5).get("stage-d"));
    }

    @Test
    void testLinearWorkflowSuccess() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(linearWorkflow, ForkJoinPool.commonPool());

        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenAnswer(stage -> CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        run.subscribe(stageStates::add);

        assertTrue(run.findOrStart().get());
        assertEquals(5, stageStates.size());
        assertEquals(Map.of("stage-a", WorkflowGraph.StageRunningState.WAITING, "stage-b", WorkflowGraph.StageRunningState.WAITING),
                stageStates.get(0));
        assertEquals(Map.of("stage-a", WorkflowGraph.StageRunningState.RUNNING, "stage-b", WorkflowGraph.StageRunningState.WAITING),
                stageStates.get(1));
        assertEquals(Map.of("stage-a", WorkflowGraph.StageRunningState.SUCCESS, "stage-b", WorkflowGraph.StageRunningState.WAITING),
                stageStates.get(2));
        assertEquals(Map.of("stage-a", WorkflowGraph.StageRunningState.SUCCESS, "stage-b", WorkflowGraph.StageRunningState.RUNNING),
                stageStates.get(3));
        assertEquals(Map.of("stage-a", WorkflowGraph.StageRunningState.SUCCESS, "stage-b", WorkflowGraph.StageRunningState.SUCCESS),
                stageStates.get(4));
    }

    @Test
    void testLinearWorkflowCached() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(linearWorkflow, ForkJoinPool.commonPool());

        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenAnswer(stage -> CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of("stage-a"), simpleExecution);
        run.subscribe(stageStates::add);

        assertTrue(run.findOrStart().get());
        assertEquals(3, stageStates.size());
        assertEquals(Map.of("stage-a", WorkflowGraph.StageRunningState.SUCCESS, "stage-b", WorkflowGraph.StageRunningState.WAITING),
                stageStates.get(0));
        assertEquals(Map.of("stage-a", WorkflowGraph.StageRunningState.SUCCESS, "stage-b", WorkflowGraph.StageRunningState.RUNNING),
                stageStates.get(1));
        assertEquals(Map.of("stage-a", WorkflowGraph.StageRunningState.SUCCESS, "stage-b", WorkflowGraph.StageRunningState.SUCCESS),
                stageStates.get(2));
    }

    @Test
    void testLinearWorkflowFails() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(linearWorkflow, ForkJoinPool.commonPool());

        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenAnswer(stage -> CompletableFuture.completedFuture(false));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        run.subscribe(stageStates::add);

        assertFalse(run.findOrStart().get());
        assertEquals(3, stageStates.size());
        assertEquals(Map.of("stage-a", WorkflowGraph.StageRunningState.WAITING, "stage-b", WorkflowGraph.StageRunningState.WAITING),
                stageStates.get(0));
        assertEquals(Map.of("stage-a", WorkflowGraph.StageRunningState.RUNNING, "stage-b", WorkflowGraph.StageRunningState.WAITING),
                stageStates.get(1));
        assertEquals(Map.of("stage-a", WorkflowGraph.StageRunningState.FAILED, "stage-b", WorkflowGraph.StageRunningState.IGNORED),
                stageStates.get(2));
    }
}
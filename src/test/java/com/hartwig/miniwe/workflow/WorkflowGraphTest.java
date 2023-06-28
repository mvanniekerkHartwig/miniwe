package com.hartwig.miniwe.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    private WorkflowDefinition inputWorkflow;

    @BeforeEach
    void setUp() throws IOException {
        simpleWorkflow = WorkflowDefinition.builder()
                .name("wf")
                .version("1.0.0")
                .addStages(Stage.builder().name("simple-stage").image("eu.gcr.io/hmf-build/image").version("1.0.0").build())
                .build();
        simpleExecution = ExecutionDefinition.builder().name("ex").workflow("wf").version("1.0.0").build();
        var classLoader = getClass().getClassLoader();
        concurrentWorkflow = new DefinitionReader().readWorkflow(classLoader.getResourceAsStream("concurrent-stage-workflow.yaml"));
        veryConcurrentWorkflow =
                new DefinitionReader().readWorkflow(classLoader.getResourceAsStream("very-concurrent-stage-workflow.yaml"));
        linearWorkflow = new DefinitionReader().readWorkflow(classLoader.getResourceAsStream("linear-stage-workflow.yaml"));
        inputWorkflow = new DefinitionReader().readWorkflow(classLoader.getResourceAsStream("workflow-with-inputs.yaml"));
    }

    @Test
    void workflowNameShouldEqualExecutionName() {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var e = assertThrows(IllegalArgumentException.class,
                () -> workflowGraph.getOrCreateRun(mock(StageScheduler.class), Set.of(), simpleExecution.withWorkflow("other-workflow")));
        assertThat(e.getMessage()).isEqualTo("Workflow name 'wf-1-0-0' should be the same as execution name, but was 'other-workflow-1-0-0'");
    }

    @Test
    void testSimpleWorkflowSucceeds() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertThat(result.get()).isTrue();
        assertThat(run.getStageStateView()).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.SUCCESS));
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

        assertThat(run.findOrStart().get()).isTrue();
        assertThat(stageStates.size()).isEqualTo(3);
        assertThat(stageStates.get(0)).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.WAITING));
        assertThat(stageStates.get(1)).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.RUNNING));
        assertThat(stageStates.get(2)).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.SUCCESS));
    }

    @Test
    void testSimpleWorkflowSucceedsGraph() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertThat(result.get()).isTrue();
        assertThat(run.toDotFormat()).isEqualTo("strict digraph G {\n" + "  1 [ label=\"simple-stage\" color=\"green\" ];\n" + "}\n");
    }

    @Test
    void testSimpleWorkflowFails() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(false));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertThat(result.get()).isFalse();
        assertThat(run.getStageStateView()).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.FAILED));
    }

    @Test
    void testSimpleWorkflowFailsSubscription() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());

        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenAnswer(stage -> CompletableFuture.completedFuture(false));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        run.subscribe(stageStates::add);

        assertThat(run.findOrStart().get()).isFalse();
        assertThat(stageStates.size()).isEqualTo(3);
        assertThat(stageStates.get(0)).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.WAITING));
        assertThat(stageStates.get(1)).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.RUNNING));
        assertThat(stageStates.get(2)).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.FAILED));
    }

    @Test
    void testSimpleWorkflowFailsGraph() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(false));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertThat(result.get()).isFalse();
        assertThat(run.toDotFormat()).isEqualTo("strict digraph G {\n" + "  1 [ label=\"simple-stage\" color=\"red\" ];\n" + "}\n");
    }

    @Test
    void sameExecutionNameReturnsSameObject() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution).findOrStart().get();
        var sameRun = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        assertThat(sameRun.getStageStateView()).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.SUCCESS));
    }

    @Test
    void sameExecutionAfterDeleteReturnsDifferentObject() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution).findOrStart().get();
        workflowGraph.delete(simpleExecution);
        var newRun = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        assertThat(newRun.getStageStateView()).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.WAITING));
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
        assertThat(success.get()).isFalse();
        assertThat(run.getStageStateView()).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.IGNORED));
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
        assertThat(success.get()).isFalse();
        assertThat(run.getStageStateView()).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.IGNORED));
    }

    @Test
    void runThatDoesNotExistCannotBeDeleted() {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var e = assertThrows(IllegalArgumentException.class, () -> workflowGraph.delete(simpleExecution));
        assertThat(e.getMessage()).isEqualTo("Could not find execution with run name 'wf-1-0-0-ex' to delete.");
    }

    @Test
    void cancellingRunBeforeStartingFails() {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var e = assertThrows(IllegalStateException.class, run::cancel);
        assertThat(e.getMessage()).isEqualTo("Cannot cancel run that was not started yet.");
    }

    @Test
    void cachedStageIsSuccess() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of("simple-stage"), simpleExecution);
        assertThat(run.findOrStart().get()).isTrue();
        assertThat(run.getStageStateView()).isEqualTo(Map.of("simple-stage", WorkflowGraph.StageRunningState.SUCCESS));
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
        assertThat(run2.get()).isFalse();
        assertThat(run1.get()).isTrue();
    }

    @Test
    void testConcurrentWorkflowSucceeds() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(concurrentWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertThat(result.get()).isTrue();
        var expected = Map.of("stage-b", WorkflowGraph.StageRunningState.SUCCESS, "stage-a", WorkflowGraph.StageRunningState.SUCCESS);
        assertThat(run.getStageStateView()).isEqualTo(expected);
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
        assertThat(result.get()).isFalse();
        var expected = Map.of("stage-b", WorkflowGraph.StageRunningState.FAILED, "stage-a", WorkflowGraph.StageRunningState.SUCCESS);
        assertThat(run.getStageStateView()).isEqualTo(expected);
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
        assertThat(result.get()).isTrue();

        assertThat(stageStates.size()).isEqualTo(6);
        assertThat(stageStates.get(0).values().stream().allMatch(state -> state == WorkflowGraph.StageRunningState.WAITING)).isTrue();
        assertThat(stageStates.get(1).values().stream().allMatch(state -> state == WorkflowGraph.StageRunningState.RUNNING)).isTrue();
        assertThat(stageStates.get(2).get("stage-a")).isEqualTo(WorkflowGraph.StageRunningState.SUCCESS);
        assertThat(stageStates.get(3).get("stage-b")).isEqualTo(WorkflowGraph.StageRunningState.SUCCESS);
        assertThat(stageStates.get(4).get("stage-c")).isEqualTo(WorkflowGraph.StageRunningState.SUCCESS);
        assertThat(stageStates.get(5).get("stage-d")).isEqualTo(WorkflowGraph.StageRunningState.SUCCESS);
    }

    @Test
    void testLinearWorkflowSuccess() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(linearWorkflow, ForkJoinPool.commonPool());

        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenAnswer(stage -> CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        run.subscribe(stageStates::add);

        assertThat(run.findOrStart().get()).isTrue();
        assertThat(stageStates.size()).isEqualTo(5);
        assertThat(stageStates.get(0)).isEqualTo(Map.of("stage-a",
                WorkflowGraph.StageRunningState.WAITING,
                "stage-b",
                WorkflowGraph.StageRunningState.WAITING));
        assertThat(stageStates.get(1)).isEqualTo(Map.of("stage-a",
                WorkflowGraph.StageRunningState.RUNNING,
                "stage-b",
                WorkflowGraph.StageRunningState.WAITING));
        assertThat(stageStates.get(2)).isEqualTo(Map.of("stage-a",
                WorkflowGraph.StageRunningState.SUCCESS,
                "stage-b",
                WorkflowGraph.StageRunningState.WAITING));
        assertThat(stageStates.get(3)).isEqualTo(Map.of("stage-a",
                WorkflowGraph.StageRunningState.SUCCESS,
                "stage-b",
                WorkflowGraph.StageRunningState.RUNNING));
        assertThat(stageStates.get(4)).isEqualTo(Map.of("stage-a",
                WorkflowGraph.StageRunningState.SUCCESS,
                "stage-b",
                WorkflowGraph.StageRunningState.SUCCESS));
    }

    @Test
    void testLinearWorkflowCached() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(linearWorkflow, ForkJoinPool.commonPool());

        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenAnswer(stage -> CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of("stage-a"), simpleExecution);
        run.subscribe(stageStates::add);

        assertThat(run.findOrStart().get()).isTrue();
        assertThat(stageStates.size()).isEqualTo(3);
        assertThat(stageStates.get(0)).isEqualTo(Map.of("stage-a",
                WorkflowGraph.StageRunningState.SUCCESS,
                "stage-b",
                WorkflowGraph.StageRunningState.WAITING));
        assertThat(stageStates.get(1)).isEqualTo(Map.of("stage-a",
                WorkflowGraph.StageRunningState.SUCCESS,
                "stage-b",
                WorkflowGraph.StageRunningState.RUNNING));
        assertThat(stageStates.get(2)).isEqualTo(Map.of("stage-a",
                WorkflowGraph.StageRunningState.SUCCESS,
                "stage-b",
                WorkflowGraph.StageRunningState.SUCCESS));
    }

    @Test
    void testLinearWorkflowFails() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(linearWorkflow, ForkJoinPool.commonPool());

        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenAnswer(stage -> CompletableFuture.completedFuture(false));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        run.subscribe(stageStates::add);

        assertThat(run.findOrStart().get()).isFalse();
        assertThat(stageStates.size()).isEqualTo(3);
        assertThat(stageStates.get(0)).isEqualTo(Map.of("stage-a",
                WorkflowGraph.StageRunningState.WAITING,
                "stage-b",
                WorkflowGraph.StageRunningState.WAITING));
        assertThat(stageStates.get(1)).isEqualTo(Map.of("stage-a",
                WorkflowGraph.StageRunningState.RUNNING,
                "stage-b",
                WorkflowGraph.StageRunningState.WAITING));
        assertThat(stageStates.get(2)).isEqualTo(Map.of("stage-a",
                WorkflowGraph.StageRunningState.FAILED,
                "stage-b",
                WorkflowGraph.StageRunningState.IGNORED));
    }

    @Test
    void testInputStageWorkflowSucceeds() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(inputWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of("input-stage"), simpleExecution);
        var result = run.findOrStart();
        assertThat(result.get()).isTrue();
        assertThat(run.getStageStateView()).isEqualTo(Map.of("input-stage",
                WorkflowGraph.StageRunningState.SUCCESS,
                "simple-stage",
                WorkflowGraph.StageRunningState.SUCCESS));
    }

    @Test
    void testInputStageFailsNoInput() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(inputWorkflow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertThat(result.get()).isFalse();
        assertThat(run.getStageStateView()).isEqualTo(Map.of("input-stage",
                WorkflowGraph.StageRunningState.FAILED,
                "simple-stage",
                WorkflowGraph.StageRunningState.IGNORED));
    }

    @Test
    void testInputStageFailsStillRunsIndependentStage() throws ExecutionException, InterruptedException {
        var workflowWithIndependentStage = simpleWorkflow.withInputStages("input-stage");
        var workflowGraph = new WorkflowGraph(workflowWithIndependentStage, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.findOrStart();
        assertThat(result.get()).isFalse();
        assertThat(run.getStageStateView()).isEqualTo(Map.of("input-stage",
                WorkflowGraph.StageRunningState.FAILED,
                "simple-stage",
                WorkflowGraph.StageRunningState.SUCCESS));
    }
}
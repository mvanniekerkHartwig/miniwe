package com.hartwig.miniwe.workflow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.ImmutableExecutionDefinition;
import com.hartwig.miniwe.miniwdl.ImmutableWorkflowDefinition;
import com.hartwig.miniwe.miniwdl.Stage;
import com.hartwig.miniwe.miniwdl.WorkflowDefinition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkflowGraphTest {

    private ImmutableWorkflowDefinition simpleWorkFlow;
    private ImmutableExecutionDefinition simpleExecution;

    @BeforeEach
    void setUp() {
        simpleWorkFlow = WorkflowDefinition.builder()
                .name("wf")
                .version("1.0.0")
                .addStages(Stage.builder().name("simple-stage").image("eu.gcr.io/hmf-build/image").version("1.0.0").build())
                .build();
        simpleExecution = ExecutionDefinition.builder().name("ex").workflow("wf").version("1.0.0").build();
    }

    @Test
    void testSimpleWorkflowSucceeds() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkFlow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.start();
        assertEquals(true, result.get());
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.SUCCESS), run.getStageStateView());
    }

    @Test
    void testSimpleWorkflowSucceedsSubscription() throws ExecutionException, InterruptedException {
        var executorService = ForkJoinPool.commonPool();
        var workflowGraph = new WorkflowGraph(simpleWorkFlow, executorService);

        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenAnswer(stage -> CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        run.subscribe(stageStates::add);

        assertEquals(true, run.start().get());
        assertEquals(stageStates.size(), 3);
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.WAITING), stageStates.get(0));
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.RUNNING), stageStates.get(1));
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.SUCCESS), stageStates.get(2));
    }

    @Test
    void testSimpleWorkflowSucceedsGraph() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkFlow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(true));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.start();
        assertEquals(true, result.get());
        assertEquals("strict digraph G {\n" + "  1 [ label=\"simple-stage\" color=\"green\" ];\n" + "}\n", run.toDotFormat());
    }

    @Test
    void testSimpleWorkflowFails() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkFlow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(false));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.start();
        assertEquals(false, result.get());
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.FAILED), run.getStageStateView());
    }

    @Test
    void testSimpleWorkflowFailsSubscription() throws ExecutionException, InterruptedException {
        var executorService = ForkJoinPool.commonPool();
        var workflowGraph = new WorkflowGraph(simpleWorkFlow, executorService);

        var stageStates = new ArrayList<Map<String, WorkflowGraph.StageRunningState>>();

        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenAnswer(stage -> CompletableFuture.completedFuture(false));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        run.subscribe(stageStates::add);

        assertEquals(false, run.start().get());
        assertEquals(stageStates.size(), 3);
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.WAITING), stageStates.get(0));
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.RUNNING), stageStates.get(1));
        assertEquals(Map.of("simple-stage", WorkflowGraph.StageRunningState.FAILED), stageStates.get(2));
    }

    @Test
    void testSimpleWorkflowFailsGraph() throws ExecutionException, InterruptedException {
        var workflowGraph = new WorkflowGraph(simpleWorkFlow, ForkJoinPool.commonPool());
        var stageScheduler = mock(StageScheduler.class);
        when(stageScheduler.schedule(any())).thenReturn(CompletableFuture.completedFuture(false));

        var run = workflowGraph.getOrCreateRun(stageScheduler, Set.of(), simpleExecution);
        var result = run.start();
        assertEquals(false, result.get());
        assertEquals("strict digraph G {\n" + "  1 [ label=\"simple-stage\" color=\"red\" ];\n" + "}\n", run.toDotFormat());
    }
}
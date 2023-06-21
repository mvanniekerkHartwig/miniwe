package com.hartwig.miniwe.workflow;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.Stage;
import com.hartwig.miniwe.miniwdl.WorkflowDefinition;

import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;
import org.jgrapht.traverse.DepthFirstIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowGraph.class);

    private final WorkflowDefinition workflowDefinition;
    private final ExecutorService executorService;
    private final ConcurrentMap<String, WorkflowGraphExecution> runsByName = new ConcurrentHashMap<>();

    public WorkflowGraph(final WorkflowDefinition workflowDefinition, final ExecutorService executorService) {
        this.workflowDefinition = workflowDefinition;
        this.executorService = executorService;
    }

    public WorkflowGraphExecution getOrCreateRun(StageScheduler stageScheduler, Set<String> cachedStages,
            ExecutionDefinition executionDefinition) {
        if (!executionDefinition.workflow().equals(workflowDefinition.name())) {
            throw new IllegalArgumentException(String.format("Workflow name '%s' should be the same as execution name, but was '%s'",
                    workflowDefinition.name(),
                    executionDefinition.workflow()));
        }
        var runName = WorkflowUtil.getRunName(executionDefinition);
        return runsByName.computeIfAbsent(runName, name -> new WorkflowGraphExecution(stageScheduler, cachedStages, executionDefinition));
    }

    /**
     * Deletes the execution. If the execution is running when it is deleted, the run will be cancelled first.
     *
     * @param executionDefinition execution definition for the run.
     */
    public void delete(ExecutionDefinition executionDefinition) {
        var runName = WorkflowUtil.getRunName(executionDefinition);
        if (!runsByName.containsKey(runName)) {
            LOGGER.warn("Could not find execution with run name '{}' to delete.", runName);
            throw new IllegalArgumentException(String.format("Could not find execution with run name '%s' to delete.", runName));
        }
        var run = runsByName.get(runName);
        if (run.isRunning()) {
            run.cancel();
        }
        runsByName.remove(runName);
    }

    private DefaultDirectedGraph<Stage, DefaultEdge> createGraph() {
        var g = new DefaultDirectedGraph<Stage, DefaultEdge>(DefaultEdge.class);
        var stageToName = new HashMap<String, Stage>();
        for (final Stage stage : workflowDefinition.stages()) {
            g.addVertex(stage);
            stageToName.put(stage.name(), stage);
        }
        for (final Stage stage : workflowDefinition.stages()) {
            for (String input : stage.inputStages()) {
                g.addEdge(stageToName.get(input), stage, new DefaultEdge());
            }
        }
        return g;
    }

    public enum StageRunningState {
        WAITING("black"),
        RUNNING("orange"),
        SUCCESS("green"),
        FAILED("red"),
        IGNORED("grey");

        final String color;

        StageRunningState(final String color) {
            this.color = color;
        }
    }

    public class WorkflowGraphExecution {
        private final Pair<Stage, Boolean> QUEUE_CANCEL_SIGNAL = Pair.of(null, null);
        private final Map<String, StageRunningState> stageTagToRunningState = new HashMap<>();
        private final BlockingQueue<Pair<Stage, Boolean>> stageDoneQueue = new LinkedBlockingQueue<>();
        private final DefaultDirectedGraph<Stage, DefaultEdge> fullGraph;
        private final DefaultDirectedGraph<Stage, DefaultEdge> runGraph;
        private final StageScheduler stageScheduler;
        private final ExecutionDefinition executionDefinition;
        private CompletableFuture<Boolean> doneFuture;

        // copy on write map, for viewing the stage state from another thread.
        private Map<String, StageRunningState> stageStateView;
        private final List<Consumer<Map<String, StageRunningState>>> stageStateSubscribers =
                Collections.synchronizedList(new ArrayList<>());

        private WorkflowGraphExecution(final StageScheduler stageScheduler, final Set<String> doneStages,
                final ExecutionDefinition executionDefinition) {
            this.stageScheduler = stageScheduler;
            this.executionDefinition = executionDefinition;
            fullGraph = createGraph();
            runGraph = createGraph();

            for (final Stage stage : fullGraph.vertexSet()) {
                if (doneStages.contains(stage.name())) {
                    LOGGER.info("[{}] Marking stage '{}' as success since the result was cached in a previous run.",
                            getRunName(),
                            stage.name());
                    runGraph.removeVertex(stage);
                    stageTagToRunningState.put(stage.name(), StageRunningState.SUCCESS);
                } else {
                    stageTagToRunningState.put(stage.name(), StageRunningState.WAITING);
                }
            }
            stageStateView = Map.copyOf(stageTagToRunningState);
        }

        /**
         * Starts the worker thread for this graph execution.
         *
         * @return Future that returns true if the whole graph finished successfully, false otherwise.
         */
        public synchronized CompletableFuture<Boolean> start() {
            if (doneFuture != null) {
                LOGGER.warn("[{}] Run was already registered. Cannot start a new run with this name.", getRunName());
                return doneFuture;
            }
            doneFuture = CompletableFuture.supplyAsync(() -> {
                updateStageStateView();
                while (!runGraph.vertexSet().isEmpty()) {
                    runRound();
                    try {
                        var done = stageDoneQueue.take();
                        if (done == QUEUE_CANCEL_SIGNAL) {
                            throw new InterruptedException("Received queue cancel signal.");
                        }
                        onStageDone(done.getLeft(), done.getRight());
                    } catch (InterruptedException e) {
                        LOGGER.warn("[{}] Workflow graph run was interrupted. Shutting down run.", getRunName());
                        onRunCancelled();
                        break;
                    }
                }
                return stageTagToRunningState.values().stream().allMatch(state -> state == StageRunningState.SUCCESS);
            }, executorService);
            return doneFuture;
        }

        public String getRunName() {
            return WorkflowUtil.getRunName(executionDefinition);
        }

        public boolean isRunning() {
            return doneFuture != null && !doneFuture.isDone();
        }

        public synchronized void cancel() {
            if (doneFuture == null) {
                throw new IllegalStateException("Can not cancel run that was not started yet.");
            }
            if (!doneFuture.isDone()) {
                stageDoneQueue.add(QUEUE_CANCEL_SIGNAL);
            }
        }

        private void runRound() {
            var readyStages = runGraph.vertexSet()
                    .stream()
                    .filter(stage -> runGraph.incomingEdgesOf(stage).size() == 0)
                    .filter(stage -> stageTagToRunningState.get(stage.name()) == StageRunningState.WAITING)
                    .collect(Collectors.toList());
            for (Stage stage : readyStages) {
                stageTagToRunningState.put(stage.name(), StageRunningState.RUNNING);
                var executionStage = ExecutionStage.from(stage, executionDefinition);
                stageScheduler.schedule(executionStage).thenAccept(result -> stageDoneQueue.add(Pair.of(stage, result)));
            }
            if (!readyStages.isEmpty()) {
                updateStageStateView();
            }
        }

        private void onStageDone(Stage stage, boolean success) {
            if (!success) {
                var iterator = new DepthFirstIterator<>(runGraph, stage);
                var ignoredStages = new ArrayList<Stage>();
                while (iterator.hasNext()) {
                    ignoredStages.add(iterator.next());
                }
                runGraph.removeAllVertices(ignoredStages);
                for (Stage ignored : ignoredStages) {
                    stageTagToRunningState.put(ignored.name(), StageRunningState.IGNORED);
                }
                stageTagToRunningState.put(stage.name(), StageRunningState.FAILED);
            } else {
                runGraph.removeVertex(stage);
                stageTagToRunningState.put(stage.name(), StageRunningState.SUCCESS);
            }
            updateStageStateView();
        }

        private void onRunCancelled() {
            for (var stage : stageTagToRunningState.entrySet()) {
                if (stage.getValue() == StageRunningState.RUNNING || stage.getValue() == StageRunningState.WAITING) {
                    stageTagToRunningState.put(stage.getKey(), StageRunningState.IGNORED);
                }
            }
            updateStageStateView();
        }

        private void updateStageStateView() {
            stageStateView = Map.copyOf(stageTagToRunningState);
            synchronized (stageStateSubscribers) {
                stageStateSubscribers.forEach(subscriber -> subscriber.accept(stageStateView));
            }
        }

        public Map<String, StageRunningState> getStageStateView() {
            return stageStateView;
        }

        public String toDotFormat() {
            var exporter = new DOTExporter<Stage, DefaultEdge>();
            exporter.setVertexAttributeProvider((v) -> {
                Map<String, Attribute> map = new LinkedHashMap<>();
                var name = v.name();
                map.put("label", DefaultAttribute.createAttribute(name));
                map.put("color", DefaultAttribute.createAttribute(stageStateView.get(name).color));
                return map;
            });
            var writer = new StringWriter();
            exporter.exportGraph(fullGraph, writer);
            return writer.toString();
        }

        public void subscribe(Consumer<Map<String, StageRunningState>> subscriber) {
            this.stageStateSubscribers.add(subscriber);
        }
    }
}

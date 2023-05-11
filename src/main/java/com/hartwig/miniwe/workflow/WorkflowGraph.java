package com.hartwig.miniwe.workflow;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.MiniWdl;
import com.hartwig.miniwe.miniwdl.Stage;

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

    private final MiniWdl pipeline;
    private final ExecutorService executorService;
    private final Map<String, WorkflowGraphExecution> runsByName = new HashMap<>();

    public WorkflowGraph(final MiniWdl pipeline, final ExecutorService executorService) {
        this.pipeline = pipeline;
        this.executorService = executorService;
    }

    public CompletableFuture<Boolean> start(StageScheduler stageScheduler, Set<String> cachedStages,
            ExecutionDefinition executionDefinition) {
        var runName = WorkflowUtil.getRunName(pipeline, executionDefinition);
        if (runsByName.get(runName) != null) {
            LOGGER.warn("Run was already started. Cannot start a new run with run name '{}' for this execution graph.", runName);
            return CompletableFuture.completedFuture(false);
        }

        var run = new WorkflowGraphExecution(stageScheduler, cachedStages, executionDefinition);
        runsByName.put(runName, run);
        return run.start();
    }

    public MiniWdl getPipeline() {
        return pipeline;
    }

    private DefaultDirectedGraph<Stage, DefaultEdge> createGraph() {
        var g = new DefaultDirectedGraph<Stage, DefaultEdge>(DefaultEdge.class);
        var stageToName = new HashMap<String, Stage>();
        for (final Stage stage : pipeline.stages()) {
            g.addVertex(stage);
            stageToName.put(stage.name(), stage);
        }
        for (final Stage stage : pipeline.stages()) {
            for (String input : stage.inputStages()) {
                g.addEdge(stageToName.get(input), stage, new DefaultEdge());
            }
        }
        return g;
    }

    private enum StageRunningState {
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

    private class WorkflowGraphExecution {
        private final Map<String, StageRunningState> stageTagToRunningState = new HashMap<>();
        private final BlockingQueue<Pair<Stage, Boolean>> stageDoneQueue = new LinkedBlockingQueue<>();
        private final DefaultDirectedGraph<Stage, DefaultEdge> fullGraph;
        private final DefaultDirectedGraph<Stage, DefaultEdge> runGraph;
        private final StageScheduler stageScheduler;
        private final ExecutionDefinition executionDefinition;

        public WorkflowGraphExecution(final StageScheduler stageScheduler, final Set<String> doneStages,
                final ExecutionDefinition executionDefinition) {
            fullGraph = createGraph();
            runGraph = createGraph();

            for (final Stage stage : fullGraph.vertexSet()) {
                if (doneStages.contains(stage.name())) {
                    LOGGER.info("Ignoring stage [{}] since the result was cached in a previous run.", stage.name());
                    runGraph.removeVertex(stage);
                    stageTagToRunningState.put(stage.name(), StageRunningState.IGNORED);
                } else {
                    stageTagToRunningState.put(stage.name(), StageRunningState.WAITING);
                }
            }
            this.stageScheduler = stageScheduler;
            this.executionDefinition = executionDefinition;
        }

        CompletableFuture<Boolean> start() {
            return CompletableFuture.supplyAsync(() -> {
                LOGGER.info("[{}] Starting execution graph. Looks like: {}", getRunName(), toDotFormat());
                while (!runGraph.vertexSet().isEmpty()) {
                    runRound();
                    try {
                        var done = stageDoneQueue.take();
                        onStageDone(done.getLeft(), done.getRight());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                return stageTagToRunningState.values().stream().allMatch(state -> state == StageRunningState.SUCCESS);
            }, executorService);
        }

        private void runRound() {
            var readyStages = runGraph.vertexSet()
                    .stream()
                    .filter(stage -> runGraph.incomingEdgesOf(stage).size() == 0)
                    .filter(stage -> stageTagToRunningState.get(stage.name()) == StageRunningState.WAITING)
                    .collect(Collectors.toList());
            for (Stage stage : readyStages) {
                LOGGER.info("Starting stage {}", stage.name());
                stageTagToRunningState.put(stage.name(), StageRunningState.RUNNING);
                var executionStage = ExecutionStage.from(stage, pipeline, executionDefinition);
                stageScheduler.schedule(executionStage).thenAccept(result -> stageDoneQueue.add(Pair.of(stage, result)));
            }
            if (!readyStages.isEmpty()) {
                LOGGER.info("[{}] Execution graph updated: {}", getRunName(), toDotFormat());
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
            LOGGER.info("[{}] Execution graph updated: {}", getRunName(), toDotFormat());
        }

        private String getRunName() {
            return pipeline.name() + "-" + executionDefinition.name();
        }

        private String toDotFormat() {
            var exporter = new DOTExporter<Stage, DefaultEdge>();
            exporter.setVertexAttributeProvider((v) -> {
                Map<String, Attribute> map = new LinkedHashMap<>();
                var name = v.name();
                map.put("label", DefaultAttribute.createAttribute(name));
                map.put("color", DefaultAttribute.createAttribute(stageTagToRunningState.get(name).color));
                return map;
            });
            var writer = new StringWriter();
            exporter.exportGraph(fullGraph, writer);
            return writer.toString();
        }
    }
}

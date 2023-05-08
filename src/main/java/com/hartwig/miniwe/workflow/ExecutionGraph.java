package com.hartwig.miniwe.workflow;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import com.hartwig.miniwe.miniwdl.MiniWdl;
import com.hartwig.miniwe.miniwdl.Stage;

import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;
import org.jgrapht.traverse.DepthFirstIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionGraph.class);

    private final MiniWdl pipeline;

    private ExecutionGraphRun run;

    public ExecutionGraph(final MiniWdl pipeline) {
        this.pipeline = pipeline;
    }

    public CompletableFuture<Boolean> start(ExecutorService executorService, StageScheduler stageScheduler) {
        if (run != null) {
            LOGGER.warn("Run was already started. Cannot start a new run for this execution graph.");
            return CompletableFuture.completedFuture(null);
        }
        run = new ExecutionGraphRun(createGraph(), executorService, stageScheduler);
        return run.start();
    }

    private DefaultDirectedGraph<Stage, NamedEdge> createGraph() {
        var g = new DefaultDirectedGraph<Stage, NamedEdge>(NamedEdge.class);
        var stageToName = new HashMap<String, Stage>();
        for (final Stage stage : pipeline.stages()) {
            g.addVertex(stage);
            stageToName.put(stage.name(), stage);
        }
        for (final Stage stage : pipeline.stages()) {
            for (String input : stage.inputStages()) {
                g.addEdge(stageToName.get(input), stage, new NamedEdge(input));
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

    private static class ExecutionGraphRun {
        private final Map<String, StageRunningState> stageTagToRunningState = new HashMap<>();
        private final BlockingQueue<Pair<Stage, Boolean>> stageDoneQueue = new LinkedBlockingQueue<>();
        private final DefaultDirectedGraph<Stage, NamedEdge> fullGraph;
        private final DefaultDirectedGraph<Stage, NamedEdge> runGraph;
        private final ExecutorService executorService;
        private final StageScheduler stageScheduler;

        public ExecutionGraphRun(final DefaultDirectedGraph<Stage, NamedEdge> g, final ExecutorService executorService,
                final StageScheduler stageScheduler) {
            fullGraph = g;
            runGraph = (DefaultDirectedGraph<Stage, NamedEdge>) g.clone();
            for (final Stage stage : g.vertexSet()) {
                stageTagToRunningState.put(stage.name(), StageRunningState.WAITING);
            }
            this.executorService = executorService;
            this.stageScheduler = stageScheduler;
        }

        CompletableFuture<Boolean> start() {
            return CompletableFuture.supplyAsync(() -> {
                LOGGER.info("Starting execution graph. Looks like: {}", toDotFormat());
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
                stageScheduler.schedule(stage.name()).thenAccept(result -> stageDoneQueue.add(Pair.of(stage, result)));
            }
            LOGGER.info("Execution graph updated: {}", toDotFormat());
        }

        private void onStageDone(Stage stage, boolean success) {
            LOGGER.info("Finished running stage {}, result was {}", stage.name(), success ? "Success" : "Fail");
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
        }

        public String toDotFormat() {
            var exporter = new DOTExporter<Stage, NamedEdge>();
            exporter.setVertexAttributeProvider((v) -> {
                Map<String, Attribute> map = new LinkedHashMap<>();
                var name = v.name();
                map.put("label", DefaultAttribute.createAttribute(name));
                map.put("color", DefaultAttribute.createAttribute(stageTagToRunningState.get(name).color));
                return map;
            });
            exporter.setEdgeAttributeProvider((e) -> {
                Map<String, Attribute> map = new LinkedHashMap<>();
                map.put("label", DefaultAttribute.createAttribute(e.name()));
                return map;
            });
            var writer = new StringWriter();
            exporter.exportGraph(fullGraph, writer);
            return writer.toString();
        }
    }
}

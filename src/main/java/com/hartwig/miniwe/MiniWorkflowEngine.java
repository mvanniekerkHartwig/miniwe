package com.hartwig.miniwe;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import com.hartwig.miniwe.gcloud.storage.GcloudStorage;
import com.hartwig.miniwe.kubernetes.KubernetesEnvironment;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.WorkflowDefinition;
import com.hartwig.miniwe.workflow.WorkflowGraph;
import com.hartwig.miniwe.workflow.WorkflowUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniWorkflowEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniWorkflowEngine.class);

    private final GcloudStorage gcloudStorage;
    private final KubernetesEnvironment kubernetesEnvironment;
    private final ExecutorService executorService;
    private final Map<String, WorkflowGraph> workflowGraphToName = new HashMap<>();

    public MiniWorkflowEngine(final GcloudStorage gcloudStorage, final KubernetesEnvironment kubernetesEnvironment,
            final ExecutorService executorService) {
        this.gcloudStorage = gcloudStorage;
        this.kubernetesEnvironment = kubernetesEnvironment;
        this.executorService = executorService;
    }

    public void addWorkflowDefinition(WorkflowDefinition miniWdl) {
        if (workflowGraphToName.containsKey(miniWdl.name())) {
            throw new IllegalArgumentException(String.format("Workflow with name '%s' already exists", miniWdl.name()));
        }
        workflowGraphToName.put(miniWdl.name(), new WorkflowGraph(miniWdl, executorService));
    }

    public CompletableFuture<Boolean> findOrStartWorkflowExecution(ExecutionDefinition executionDefinition) {
        var runName = WorkflowUtil.getRunName(executionDefinition);
        LOGGER.info("[{}] Starting run", runName);
        var workflowGraph = workflowGraphToName.get(executionDefinition.workflow());
        if (workflowGraph == null) {
            throw new IllegalArgumentException(String.format("Workflow with name '%s' does not exist.", executionDefinition.workflow()));
        }
        var bucket = gcloudStorage.findOrCreateBucket(runName);
        var scheduler = kubernetesEnvironment.findOrCreateScheduler(runName, bucket);
        var run = workflowGraph.getOrCreateRun(scheduler, bucket.getCachedStages(), executionDefinition);
        run.subscribe(stage -> LOGGER.info("[{}] Execution graph updated: {}", run.getRunName(), run.toDotFormat()));
        return run.start();
    }

    public void cleanupWorkflowExecution(ExecutionDefinition executionDefinition) {
        var runName = WorkflowUtil.getRunName(executionDefinition);
        LOGGER.info("Cleaning up run with name '{}'", runName);
        var workflowGraph = workflowGraphToName.get(executionDefinition.workflow());
        if (workflowGraph == null) {
            throw new IllegalArgumentException(String.format("Workflow with name '%s' does not exist.", executionDefinition.workflow()));
        }
        workflowGraph.delete(executionDefinition);
        kubernetesEnvironment.deleteScheduledResources(runName);
        gcloudStorage.deleteBucket(runName);
    }
}

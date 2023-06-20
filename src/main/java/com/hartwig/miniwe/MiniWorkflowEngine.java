package com.hartwig.miniwe;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import com.hartwig.miniwe.gcloud.storage.GcloudStorage;
import com.hartwig.miniwe.kubernetes.KubernetesEnvironment;
import com.hartwig.miniwe.kubernetes.KubernetesStageScheduler;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.WorkflowDefinition;
import com.hartwig.miniwe.workflow.WorkflowGraph;
import com.hartwig.miniwe.workflow.WorkflowUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniWorkflowEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniWorkflowEngine.class);

    private final GcloudStorage gcloudStorage;
    private final KubernetesStageScheduler kubernetesStageScheduler;
    private final ExecutorService executorService;
    private final ConcurrentMap<String, WorkflowGraph> workflowGraphToName = new ConcurrentHashMap<>();

    public MiniWorkflowEngine(final GcloudStorage gcloudStorage, final KubernetesStageScheduler kubernetesStageScheduler,
            final ExecutorService executorService) {
        this.gcloudStorage = gcloudStorage;
        this.kubernetesStageScheduler = kubernetesStageScheduler;
        this.executorService = executorService;
    }

    public void addWorkflowDefinition(WorkflowDefinition workflowDefinition) {
        if (workflowGraphToName.containsKey(workflowDefinition.name())) {
            throw new IllegalArgumentException(String.format("Workflow with name '%s' already exists", workflowDefinition.name()));
        }
        workflowGraphToName.put(workflowDefinition.name(), new WorkflowGraph(workflowDefinition, executorService));
    }

    public CompletableFuture<Boolean> findOrStartRun(ExecutionDefinition executionDefinition) {
        var runName = WorkflowUtil.getRunName(executionDefinition);
        LOGGER.info("[{}] Starting run", runName);
        var workflowGraph = workflowGraphToName.get(executionDefinition.workflow());
        if (workflowGraph == null) {
            throw new IllegalArgumentException(String.format("Workflow with name '%s' does not exist.", executionDefinition.workflow()));
        }
        var bucket = gcloudStorage.findOrCreateBucket(runName);
        var run = workflowGraph.getOrCreateRun(kubernetesStageScheduler, bucket.getCachedStages(), executionDefinition);
        run.subscribe(stage -> LOGGER.info("[{}] Execution graph updated: {}", run.getRunName(), run.toDotFormat()));
        return run.start();
    }

    public void cleanupRun(ExecutionDefinition executionDefinition) {
        var runName = WorkflowUtil.getRunName(executionDefinition);
        LOGGER.info("Cleaning up run with name '{}'", runName);
        var workflowGraph = workflowGraphToName.get(executionDefinition.workflow());
        if (workflowGraph == null) {
            throw new IllegalArgumentException(String.format("Workflow with name '%s' does not exist.", executionDefinition.workflow()));
        }
        workflowGraph.delete(executionDefinition);
        kubernetesStageScheduler.deleteStagesForRun(executionDefinition);
        gcloudStorage.deleteBucket(runName);
    }
}

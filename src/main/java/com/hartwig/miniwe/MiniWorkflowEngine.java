package com.hartwig.miniwe;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import com.hartwig.miniwe.gcloud.storage.GcloudStorage;
import com.hartwig.miniwe.kubernetes.KubernetesStageScheduler;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.WorkflowDefinition;
import com.hartwig.miniwe.workflow.WorkflowGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniWorkflowEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniWorkflowEngine.class);
    private static final int MAX_CONCURRENT_RUNS = 32;

    private final GcloudStorage gcloudStorage;
    private final KubernetesStageScheduler kubernetesStageScheduler;
    private final ExecutorService executorService;
    private final ConcurrentMap<String, WorkflowGraph> workflowGraphByName = new ConcurrentHashMap<>();

    public MiniWorkflowEngine(GcloudStorage gcloudStorage, KubernetesStageScheduler kubernetesStageScheduler) {
        this.gcloudStorage = gcloudStorage;
        this.kubernetesStageScheduler = kubernetesStageScheduler;
        this.executorService = ExecutorUtil.createExecutorService(MAX_CONCURRENT_RUNS, "workflow-run-thread-%d");
    }

    public void addWorkflowDefinition(WorkflowDefinition workflowDefinition) {
        var workflowName = workflowDefinition.getWorkflowName();
        if (workflowGraphByName.containsKey(workflowName)) {
            throw new IllegalArgumentException(String.format("Workflow with name '%s' already exists", workflowName));
        }
        workflowGraphByName.put(workflowName, new WorkflowGraph(workflowDefinition, executorService));
    }

    public CompletableFuture<Boolean> findOrStartRun(ExecutionDefinition executionDefinition) {
        return findOrStartRun(executionDefinition, Map.of());
    }

    public CompletableFuture<Boolean> findOrStartRun(ExecutionDefinition executionDefinition, Map<String, String> secretsByEnvVariable) {
        LOGGER.info("[{}] Starting run", executionDefinition.getRunName());
        WorkflowGraph workflowGraph = getWorkflowGraph(executionDefinition);
        var bucket = gcloudStorage.findOrCreateBucket(executionDefinition.getBucketName());
        var run = workflowGraph.getOrCreateRun(kubernetesStageScheduler, bucket.getCachedStages(), executionDefinition, secretsByEnvVariable);
        run.subscribe(stage -> LOGGER.info("[{}] Execution graph updated: {}", run.getRunName(), run.toDotFormat()));
        return run.findOrStart();
    }

    @SuppressWarnings("unused")
    public void cleanupRun(ExecutionDefinition executionDefinition) {
        LOGGER.info("[{}] Cleaning up run", executionDefinition.getRunName());
        var workflowGraph = getWorkflowGraph(executionDefinition);
        workflowGraph.delete(executionDefinition);
        kubernetesStageScheduler.deleteStagesForRun(executionDefinition);
    }

    private WorkflowGraph getWorkflowGraph(final ExecutionDefinition executionDefinition) {
        var workflowName = executionDefinition.getWorkflowName();
        var workflowGraph = workflowGraphByName.get(workflowName);
        if (workflowGraph == null) {
            throw new IllegalArgumentException(String.format("Workflow with name '%s' does not exist.", workflowName));
        }
        return workflowGraph;
    }
}

package com.hartwig.miniwe;

import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;

import com.google.cloud.storage.StorageOptions;
import com.hartwig.miniwe.gcloud.storage.GcloudStorage;
import com.hartwig.miniwe.kubernetes.KubernetesClientWrapper;
import com.hartwig.miniwe.kubernetes.KubernetesStageScheduler;
import com.hartwig.miniwe.miniwdl.DefinitionReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import picocli.CommandLine;

public class MiniWeMain implements Callable<Integer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniWeMain.class);

    @CommandLine.Parameters(paramLabel = "workflow_description_yaml",
                            index = "0",
                            description = "Path to the workflow description file")
    private String workflowDescriptionYaml;

    @CommandLine.Parameters(paramLabel = "execution_definition_yaml",
                            index = "1",
                            description = "Path to the execution definition file")
    private String executionDefinitionYaml;

    @CommandLine.Option(names = { "--kubernetes-namespace" },
                        defaultValue = "default",
                        description = "Name of the kubernetes namespace")
    private String kubernetesNamespace;

    @CommandLine.Option(names = { "--service-account-name" },
                        description = "Name of the kubernetes job service account")
    private String kubernetesServiceAccountName;

    @CommandLine.Option(names = { "--gcp-project-id" },
                        description = "Name of the GCP project ID")
    private String gcpProjectId;

    @CommandLine.Option(names = { "--gcp-region" },
                        description = "Name of the GCP region")
    private String gcpRegion;

    @Override
    public Integer call() {
        try (var kubernetesClient = new KubernetesClientBuilder().build();
                var gcloudStorage = StorageOptions.newBuilder().setProjectId(gcpProjectId).build().getService()) {
            var definitionReader = new DefinitionReader();
            var executionDefinition = definitionReader.readExecution(executionDefinitionYaml);
            var workflowDefinition = definitionReader.readWorkflow(workflowDescriptionYaml);

            var executorService = ForkJoinPool.commonPool();
            var storage = new GcloudStorage(gcloudStorage, gcpRegion);
            var kubernetesClientWrapper = new KubernetesClientWrapper(kubernetesClient);
            var kubernetesStageScheduler = new KubernetesStageScheduler(kubernetesNamespace,
                    executorService,
                    kubernetesClientWrapper,
                    kubernetesServiceAccountName,
                    storage);
            var miniWorkflowEngine = new MiniWorkflowEngine(storage, kubernetesStageScheduler, executorService);

            miniWorkflowEngine.addWorkflowDefinition(workflowDefinition);

            LOGGER.info("Starting execution graph.");
            var success = miniWorkflowEngine.findOrStartRun(executionDefinition).get();
            LOGGER.info("Finished running execution graph. Final result: {}.", success ? "Success" : "Failed");
            if (success) {
                miniWorkflowEngine.cleanupRun(executionDefinition);
            }
            return 0;
        } catch (Exception e) {
            LOGGER.error("Unexpected exception", e);
            return 1;
        }

    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new MiniWeMain()).execute(args));
    }
}
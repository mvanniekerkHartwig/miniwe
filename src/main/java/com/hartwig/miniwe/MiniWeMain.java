package com.hartwig.miniwe;

import java.io.FileInputStream;
import java.util.concurrent.Callable;

import com.google.cloud.storage.StorageOptions;
import com.hartwig.miniwe.gcloud.storage.GcloudStorage;
import com.hartwig.miniwe.kubernetes.BlockingKubernetesClient;
import com.hartwig.miniwe.kubernetes.KubernetesStageScheduler;
import com.hartwig.miniwe.miniwdl.DefinitionReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;

public class MiniWeMain implements Callable<Integer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MiniWeMain.class);

    @CommandLine.Parameters(paramLabel = "workflow_yaml",
                            index = "0",
                            description = "Path to the workflow definition file")
    private String workflowYamlFileName;

    @CommandLine.Parameters(paramLabel = "execution_yaml",
                            index = "1",
                            description = "Path to the execution definition file")
    private String executionYamlFileName;

    @CommandLine.Option(names = { "--k8s-namespace" },
                        defaultValue = "default",
                        description = "Name of the kubernetes namespace")
    private String kubernetesNamespace;

    @CommandLine.Option(names = { "--k8s-stage-copy-sa" },
                        description = "Name of the kubernetes stage copy service account")
    private String kubernetesServiceAccountName;

    @CommandLine.Option(names = { "--gcp-project-id" },
                        description = "Name of the GCP project ID")
    private String gcpProjectId;

    @CommandLine.Option(names = { "--gcp-region" },
                        description = "Name of the GCP region")
    private String gcpRegion;

    @Override
    public Integer call() {
        try (var blockingKubernetesClient = new BlockingKubernetesClient();
                var gcloudStorage = StorageOptions.newBuilder().setProjectId(gcpProjectId).build().getService();
                var executionDefinitionYaml = new FileInputStream(executionYamlFileName);
                var workflowDescriptionYaml = new FileInputStream(workflowYamlFileName)) {

            var definitionReader = new DefinitionReader();
            var executionDefinition = definitionReader.readExecution(executionDefinitionYaml);
            var workflowDefinition = definitionReader.readWorkflow(workflowDescriptionYaml);

            var storage = new GcloudStorage(gcloudStorage, gcpRegion);
            var kubernetesStageScheduler =
                    new KubernetesStageScheduler(kubernetesNamespace, blockingKubernetesClient, kubernetesServiceAccountName);
            var miniWorkflowEngine = new MiniWorkflowEngine(storage, kubernetesStageScheduler);

            miniWorkflowEngine.addWorkflowDefinition(workflowDefinition);

            LOGGER.info("Starting execution graph.");
            var success = miniWorkflowEngine.findOrStartRun(executionDefinition).get();
            LOGGER.info("Finished running execution graph. Final result: {}.", success ? "Success" : "Failed");
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
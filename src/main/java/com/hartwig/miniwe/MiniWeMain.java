package com.hartwig.miniwe;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.storage.StorageOptions;
import com.hartwig.miniwe.gcloud.storage.GcloudStorage;
import com.hartwig.miniwe.kubernetes.KubernetesEnvironment;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.MiniWdl;

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
            ObjectMapper mapper = getObjectMapper();
            var executionDefinition = mapper.readValue(new File(executionDefinitionYaml), ExecutionDefinition.class);
            var miniWdl = mapper.readValue(new File(workflowDescriptionYaml), MiniWdl.class);

            var executorService = ForkJoinPool.commonPool();
            var storage = new GcloudStorage(gcloudStorage, gcpRegion);
            var kubernetesEnvironment =
                    new KubernetesEnvironment(kubernetesNamespace, executorService, kubernetesClient, kubernetesServiceAccountName);
            var miniWorkflowEngine = new MiniWorkflowEngine(storage, kubernetesEnvironment, executorService);

            miniWorkflowEngine.addWorkflowDefinition(miniWdl);

            LOGGER.info("Starting execution graph.");
            var success = miniWorkflowEngine.findOrStartWorkflowExecution(executionDefinition).get();
            LOGGER.info("Finished running execution graph. Final result: {}.", success ? "Success" : "Failed");
            if (success) {
                miniWorkflowEngine.cleanupWorkflowExecution(executionDefinition);
            }
            return 0;
        } catch (Exception e) {
            LOGGER.error("Unexpected exception", e);
            return 1;
        }

    }

    private static ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new MiniWeMain()).execute(args));
    }
}
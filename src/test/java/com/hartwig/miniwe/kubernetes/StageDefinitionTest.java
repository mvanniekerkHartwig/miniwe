package com.hartwig.miniwe.kubernetes;

import static com.hartwig.miniwe.kubernetes.KubernetesStageScheduler.DEFAULT_STORAGE_SIZE_GI;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.ImmutableExecutionDefinition;
import com.hartwig.miniwe.miniwdl.ImmutableStage;
import com.hartwig.miniwe.miniwdl.Stage;
import com.hartwig.miniwe.miniwdl.StageOptions;
import com.hartwig.miniwe.workflow.ExecutionStage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StageDefinitionTest {
    private ImmutableExecutionDefinition simpleExecution;
    private ImmutableStage simpleStage;
    private final String namespace = "namespace";
    private final String serviceAccountName = "serviceAccount";
    private final StorageProvider storageProvider = new GcloudStorageProvider();

    @BeforeEach
    void setUp() {
        simpleStage = Stage.builder().name("simple-stage").image("eu.gcr.io/hmf-build/image").version("1.0.0").build();
        simpleExecution = ExecutionDefinition.builder().name("ex").workflow("wf").version("1.0.0").build();
    }

    @Test
    void simpleStageTest() throws IOException {
        var simpleExecutionStage = ExecutionStage.from(simpleStage, simpleExecution);
        var stageDefinition =
                new StageDefinition(simpleExecutionStage, namespace, DEFAULT_STORAGE_SIZE_GI, serviceAccountName, storageProvider);
        assertThat(stageDefinition.getStageName()).isEqualTo("wf-1-0-0-ex-simple-stage");
        assertThat(stageDefinition.toString()).isEqualTo(readResourceAsString("k8s/simple-stage-k8s.yaml"));
    }

    @Test
    void stageWithInputTest() throws IOException {
        var withInputStage = simpleStage.withInputStages("stage-a", "stage-b");
        var simpleExecutionStage = ExecutionStage.from(withInputStage, simpleExecution);
        var stageDefinition =
                new StageDefinition(simpleExecutionStage, namespace, DEFAULT_STORAGE_SIZE_GI, serviceAccountName, storageProvider);
        assertThat(stageDefinition.toString()).isEqualTo(readResourceAsString("k8s/stage-with-input-k8s.yaml"));
    }

    @Test
    void stageNoOutputTest() throws IOException {
        var noOutputStage = simpleStage.withOptions(StageOptions.builder().output(false).build());
        var simpleExecutionStage = ExecutionStage.from(noOutputStage, simpleExecution);
        var stageDefinition =
                new StageDefinition(simpleExecutionStage, namespace, DEFAULT_STORAGE_SIZE_GI, serviceAccountName, storageProvider);
        assertThat(stageDefinition.toString()).isEqualTo(readResourceAsString("k8s/stage-no-output-k8s.yaml"));
    }

    @Test
    void stageWithServiceAccountTest() throws IOException {
        var serviceAccountStage = simpleStage.withOptions(StageOptions.builder().serviceAccount("my-sa").build());
        var simpleExecutionStage = ExecutionStage.from(serviceAccountStage, simpleExecution);
        var stageDefinition =
                new StageDefinition(simpleExecutionStage, namespace, DEFAULT_STORAGE_SIZE_GI, serviceAccountName, storageProvider);
        assertThat(stageDefinition.toString()).isEqualTo(readResourceAsString("k8s/stage-sa-k8s.yaml"));
    }

    @Test
    void stageWithSecretTest() throws IOException {
        var secretStage = simpleStage.withSecrets("PASSWORD");
        var simpleExecutionStage = ExecutionStage.from(secretStage, simpleExecution, Map.of("PASSWORD", "my-password"));
        var stageDefinition =
                new StageDefinition(simpleExecutionStage, namespace, DEFAULT_STORAGE_SIZE_GI, serviceAccountName, storageProvider);
        assertThat(stageDefinition.toString()).isEqualTo(readResourceAsString("k8s/stage-with-secret-k8s.yaml"));
    }

    private String readResourceAsString(String filename) throws IOException {
        try (var is = getClass().getClassLoader().getResourceAsStream(filename)) {
            if (is == null) {
                throw new IOException("Resource could not be found.");
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
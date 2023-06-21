package com.hartwig.miniwe.kubernetes;

import static com.hartwig.miniwe.kubernetes.KubernetesStageScheduler.DEFAULT_STORAGE_SIZE_GI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.stream.Collectors;

import com.google.api.client.util.IOUtils;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.io.CharStreams;
import com.hartwig.miniwe.gcloud.storage.GcloudBucket;
import com.hartwig.miniwe.gcloud.storage.GcloudStorage;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.ImmutableExecutionDefinition;
import com.hartwig.miniwe.miniwdl.ImmutableStage;
import com.hartwig.miniwe.miniwdl.Stage;
import com.hartwig.miniwe.workflow.ExecutionStage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StageDefinitionTest {
    private ImmutableExecutionDefinition simpleExecution;
    private ImmutableStage simpleStage;
    private final String namespace = "namespace";
    private final String serviceAccountName = "serviceAccount";
    private GcloudStorage storageProvider;

    @BeforeEach
    void setUp() {
        simpleStage = Stage.builder().name("simple-stage").image("eu.gcr.io/hmf-build/image").version("1.0.0").build();
        simpleExecution = ExecutionDefinition.builder().name("ex").workflow("wf").version("1.0.0").build();
        var bucketMock = mock(Bucket.class);
        when(bucketMock.getName()).thenReturn("bucket-name");
        var storageMock = mock(Storage.class);
        when(storageMock.get(any(String.class))).thenReturn(bucketMock);
        storageProvider = new GcloudStorage(storageMock, "");
    }

    @Test
    void simpleStageTest() throws IOException {
        var simpleExecutionStage = ExecutionStage.from(simpleStage, simpleExecution);
        var stageDefinition =
                new StageDefinition(simpleExecutionStage, namespace, DEFAULT_STORAGE_SIZE_GI, serviceAccountName, storageProvider);
        assertEquals("wf-1-0-0-ex-simple-stage", stageDefinition.getStageName());
        assertEquals(readResourceAsString("simple-stage-k8s.yaml"), stageDefinition.toString());
    }

    @Test
    void simpleStageWithInputTest() throws IOException {
        var withInputStage = simpleStage.withInputStages("stage-a", "stage-b");
        var simpleExecutionStage = ExecutionStage.from(withInputStage, simpleExecution);
        var stageDefinition =
                new StageDefinition(simpleExecutionStage, namespace, DEFAULT_STORAGE_SIZE_GI, serviceAccountName, storageProvider);
        assertEquals("wf-1-0-0-ex-simple-stage", stageDefinition.getStageName());
        assertEquals(readResourceAsString("simple-stage-with-input-k8s.yaml"), stageDefinition.toString());
    }

    private String readResourceAsString(String filename) throws IOException {
        try (var is = getClass().getClassLoader().getResourceAsStream(filename)) {
            return CharStreams.toString(new InputStreamReader(is));
        }
    }
}
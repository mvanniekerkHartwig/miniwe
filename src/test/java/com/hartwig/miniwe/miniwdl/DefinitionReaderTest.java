package com.hartwig.miniwe.miniwdl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefinitionReaderTest {
    private InputStream executionInputStream;
    private InputStream workFlowInputStream;
    private final DefinitionReader definitionReader = new DefinitionReader();

    @BeforeEach
    void setUp() {
        var classLoader = getClass().getClassLoader();
        executionInputStream = classLoader.getResourceAsStream("real-execution.yaml");
        workFlowInputStream = classLoader.getResourceAsStream("real-workflow.yaml");
    }

    @Test
    void parsingRealWorkflowDoesNotThrow() throws IOException {
        definitionReader.readWorkflow(workFlowInputStream);
    }

    @Test
    void parsingRealExecutionDoesNotThrow() throws IOException {
        definitionReader.readExecution(executionInputStream);
    }

    @Test
    void parseSimpleExecution() throws IOException {
        String simpleExecution = "name: \"simple\"\nworkflow: \"wf\"\nversion: \"1.0.0\"";
        var result = definitionReader.readExecution(new ByteArrayInputStream(simpleExecution.getBytes(StandardCharsets.UTF_8)));
        var expected = ExecutionDefinition.builder().name("simple").workflow("wf").version("1.0.0").build();
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void extraFieldThrows() {
        String extraFieldExecution = "name: \"simple\"\nworkflow: \"wf\"\nversion: \"1.0.0\"\nworkflowName: \"field\"";
        assertThrows(UnrecognizedPropertyException.class,
                () -> definitionReader.readExecution(new ByteArrayInputStream(extraFieldExecution.getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void parseSimpleWorkflow() throws IOException {
        var result = definitionReader.readWorkflow(getClass().getClassLoader().getResourceAsStream("simple-workflow.yaml"));
        var expected = WorkflowDefinition.builder()
                .name("wf")
                .version("1.0.0")
                .addStages(Stage.builder().name("simple-stage").image("eu.gcr.io/hmf-build/image").version("1.0.0").build())
                .build();
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void parseWorkflowWithOptions() throws IOException {
        var result = definitionReader.readWorkflow(getClass().getClassLoader().getResourceAsStream("workflow-with-stage-options.yaml"));
        var stage = Stage.builder()
                .name("simple-stage")
                .image("eu.gcr.io/hmf-build/image")
                .version("1.0.0")
                .options(StageOptions.builder().serviceAccount("my-service-account").output(false).build())
                .build();
        var expected = WorkflowDefinition.builder().name("wf").version("1.0.0").addStages(stage).build();
        assertThat(result).isEqualTo(expected);
    }
}
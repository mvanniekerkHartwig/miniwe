package com.hartwig.miniwe.miniwdl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefinitionReaderTest {
    private InputStream executionInputStream;
    private InputStream workFlowInputStream;
    private final DefinitionReader definitionReader = new DefinitionReader();

    @BeforeEach
    void setUp() {
        var classLoader = getClass().getClassLoader();
        executionInputStream = classLoader.getResourceAsStream("execution.yaml");
        workFlowInputStream = classLoader.getResourceAsStream("workflow.yaml");
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
        var executionDefinition = ExecutionDefinition.builder().name("simple").workflow("wf").version("1.0.0").build();
        assertEquals(result, executionDefinition);
    }

    @Test
    void parseSimpleWorkflow() throws IOException {
        var result = definitionReader.readWorkflow(getClass().getClassLoader().getResourceAsStream("simple-workflow.yaml"));
        var workflow = WorkflowDefinition.builder()
                .name("simple")
                .version("1.0.0")
                .addStages(Stage.builder().name("simple-stage").image("eu.gcr.io/hmf-build/image").version("1.0.0").build())
                .build();
        assertEquals(result, workflow);
    }
}
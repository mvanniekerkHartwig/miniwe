package com.hartwig.miniwe.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.ImmutableExecutionDefinition;
import com.hartwig.miniwe.miniwdl.ImmutableStage;
import com.hartwig.miniwe.miniwdl.Stage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExecutionStageTest {
    private ImmutableExecutionDefinition simpleExecution;
    private ImmutableStage simpleStage;

    @BeforeEach
    void setUp() {
        simpleStage = Stage.builder().name("simple-stage").image("eu.gcr.io/hmf-build/image").version("1.0.0").build();
        simpleExecution = ExecutionDefinition.builder().name("ex").workflow("wf").version("1.0.0").build();
    }

    @Test
    void testSimpleStage() {
        var executionStage = ExecutionStage.from(simpleStage, simpleExecution);
        assertEquals(simpleStage, executionStage.stage());
        assertEquals("wf-ex", executionStage.runName());
    }

    @Test
    void stageKeysGetReplacedInArgument() {
        var paramExecution = simpleExecution.withParams(Map.of("param", "1"));
        var paramStage = simpleStage.withArguments("${param} --flag");
        var executionStage = ExecutionStage.from(paramStage, paramExecution);

        assertEquals("1 --flag", executionStage.stage().arguments().get());
    }

    @Test
    void stageKeysGetReplacedInEntrypoint() {
        var paramExecution = simpleExecution.withParams(Map.of("param", "1"));
        var paramStage = simpleStage.withEntrypoint("${param} --flag");
        var executionStage = ExecutionStage.from(paramStage, paramExecution);

        assertEquals("1 --flag", executionStage.stage().entrypoint().get());
    }
}
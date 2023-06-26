package com.hartwig.miniwe.workflow;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(executionStage.stage()).isEqualTo(simpleStage);
        assertThat(executionStage.runName()).isEqualTo("wf-1-0-0-ex");
    }

    @Test
    void stageKeysGetReplacedInArgument() {
        var paramExecution = simpleExecution.withParams(Map.of("param", "1"));
        var paramStage = simpleStage.withArguments("${param} --flag");
        var executionStage = ExecutionStage.from(paramStage, paramExecution);

        assertThat(executionStage.stage().arguments().get()).isEqualTo("1 --flag");
    }

    @Test
    void stageKeysGetReplacedInCommand() {
        var paramExecution = simpleExecution.withParams(Map.of("param", "1"));
        var paramStage = simpleStage.withCommand("${param} --flag");
        var executionStage = ExecutionStage.from(paramStage, paramExecution);

        assertThat(executionStage.stage().command().get()).isEqualTo("1 --flag");
    }
}
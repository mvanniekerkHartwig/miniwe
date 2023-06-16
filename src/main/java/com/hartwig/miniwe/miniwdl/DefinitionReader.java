package com.hartwig.miniwe.miniwdl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

public class DefinitionReader {
    private final ObjectMapper objectMapper;

    public DefinitionReader() {
        objectMapper = new ObjectMapper(new YAMLFactory());
        objectMapper.registerModule(new Jdk8Module());
    }

    public ExecutionDefinition readExecution(String executionDefinitionFileName) throws IOException {
        return readExecution(new FileInputStream(executionDefinitionFileName));
    }

    public ExecutionDefinition readExecution(InputStream executionDefinition) throws IOException {
        return objectMapper.readValue(executionDefinition, ExecutionDefinition.class);
    }

    public WorkflowDefinition readWorkflow(InputStream workflowDefinition) throws IOException {
        return objectMapper.readValue(workflowDefinition, WorkflowDefinition.class);
    }

    public WorkflowDefinition readWorkflow(String workflowDefinitionFileName) throws IOException {
        return readWorkflow(new FileInputStream(workflowDefinitionFileName));
    }
}

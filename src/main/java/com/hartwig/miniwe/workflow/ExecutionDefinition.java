package com.hartwig.miniwe.workflow;

import java.util.Map;

public interface ExecutionDefinition {
    String executionName();
    Map<String, String> params();
}

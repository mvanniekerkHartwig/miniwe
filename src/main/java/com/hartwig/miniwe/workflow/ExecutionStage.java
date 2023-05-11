package com.hartwig.miniwe.workflow;

import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.MiniWdl;
import com.hartwig.miniwe.miniwdl.Stage;

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(jdkOnly = true)
@JsonDeserialize(as = ImmutableExecutionStage.class)
public interface ExecutionStage {
    Stage stage();

    String runName();

    static ExecutionStage from(Stage stage, MiniWdl pipeline, ExecutionDefinition execution) {
        var replaced = replaced(stage, execution.params());
        return ImmutableExecutionStage.builder().stage(replaced).runName(pipeline.name() + "-" + execution.name()).build();
    }

    private static Stage replaced(final Stage stage, final Map<String, String> map) {
        var arguments = stage.arguments().map(argument -> replaceKeys(argument, map));
        var entryPoints = stage.entrypoint().map(entryPoint -> replaceKeys(entryPoint, map));
        return Stage.builder().from(stage).arguments(arguments).entrypoint(entryPoints).build();
    }

    private static String replaceKeys(final String input, final Map<String, String> map) {
        var output = input;
        for (var entry : map.entrySet()) {
            var key = "${" + entry.getKey() + "}";
            output = output.replace(key, entry.getValue());
        }
        return output;
    }
}

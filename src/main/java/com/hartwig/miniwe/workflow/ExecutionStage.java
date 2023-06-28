package com.hartwig.miniwe.workflow;

import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.hartwig.miniwe.kubernetes.KubernetesUtil;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.Stage;

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(jdkOnly = true)
@JsonDeserialize(as = ImmutableExecutionStage.class)
@JsonSerialize(as = ImmutableExecutionStage.class)
public interface ExecutionStage {
    Stage stage();

    String runName();

    String bucketName();

    default String getName() {
        return KubernetesUtil.toValidRFC1123Label(runName(), stage().name());
    }

    static ExecutionStage from(Stage stage, ExecutionDefinition execution) {
        var replaced = replaced(stage, execution.params());
        return ImmutableExecutionStage.builder()
                .stage(replaced)
                .runName(execution.getRunName())
                .bucketName(execution.getBucketName())
                .build();
    }

    private static Stage replaced(Stage stage, Map<String, String> map) {
        var arguments = stage.arguments().map(argument -> replaceKeys(argument, map));
        var commands = stage.command().map(command -> replaceKeys(command, map));
        return Stage.builder().from(stage).arguments(arguments).command(commands).build();
    }

    private static String replaceKeys(String input, Map<String, String> map) {
        var output = input;
        for (var entry : map.entrySet()) {
            var key = "${" + entry.getKey() + "}";
            output = output.replace(key, entry.getValue());
        }
        return output;
    }
}

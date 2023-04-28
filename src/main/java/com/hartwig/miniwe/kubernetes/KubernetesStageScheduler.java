package com.hartwig.miniwe.kubernetes;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import com.hartwig.miniwe.miniwdl.MiniWdl;
import com.hartwig.miniwe.miniwdl.Stage;
import com.hartwig.miniwe.workflow.ExecutionDefinition;
import com.hartwig.miniwe.workflow.StageScheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClient;

public class KubernetesStageScheduler implements StageScheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesStageScheduler.class);
    public static final int DEFAULT_STORAGE_SIZE_GI = 1;

    private final String namespace;
    private final ExecutionDefinition executionDefinition;
    private final MiniWdl miniWdl;
    private final ExecutorService executor;
    private final KubernetesClient kubernetesClient;

    public KubernetesStageScheduler(final String namespace, final ExecutionDefinition executionDefinition, final MiniWdl miniWdl,
            final ExecutorService executor, final KubernetesClient kubernetesClient) {
        this.namespace = namespace;
        this.executionDefinition = executionDefinition;
        this.miniWdl = miniWdl;
        this.executor = executor;
        this.kubernetesClient = kubernetesClient;
    }

    @Override
    public CompletableFuture<Boolean> schedule(final String stageName) {
        return CompletableFuture.supplyAsync(() -> {
            var stage = replaced(getStage(stageName), executionDefinition.params());
            var runName = executionDefinition.name();
            var definition = new StageDefinition(stage, runName, miniWdl.name(), namespace, DEFAULT_STORAGE_SIZE_GI);
            try (var run = definition.submit(kubernetesClient)) {
                run.start();
                run.waitUntilComplete();
                LOGGER.info("Stage [{}] completed", definition.getStageName());
                return true;
            } catch (Exception e) {
                LOGGER.error("Stage [{}] failed with", definition.getStageName(), e);
                return false;
            }
        }, executor);
    }

    private Stage getStage(final String stageName) {
        return miniWdl.stages()
                .stream()
                .filter(s -> s.name().equals(stageName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Invalid stage name '%s' for definition: %s",
                        stageName,
                        miniWdl)));
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

package com.hartwig.miniwe.kubernetes;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.miniwe.miniwdl.StageOptions;
import com.hartwig.miniwe.workflow.ExecutionStage;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimSpecBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpecBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;

public class StageDefinition {
    private final String stageName;
    private PersistentVolumeClaim pvc = null;
    private Job inputJob = null;
    private Secret secret = null;
    private final Job job;
    private Job onCompleteCopyJob = null;

    public StageDefinition(ExecutionStage executionStage, String namespace, int storageSizeGi, String stageCopyServiceAccount,
            StorageProvider storageProvider) {
        this.stageName = executionStage.getName();
        var stage = executionStage.stage();
        var imageName = String.format("%s:%s", stage.image(), stage.version());

        var args = stage.arguments().map(arguments -> List.of(arguments.split(" ")));
        var command = stage.command().map(cmd -> List.of(cmd.split(" ")));

        List<EnvVar> environmentVariables = new ArrayList<>();
        List<Volume> volumes = new ArrayList<>();
        List<VolumeMount> mounts = new ArrayList<>();

        var hasInput = !stage.inputStages().isEmpty();
        var hasOutput = executionStage.stage().options().map(StageOptions::output).orElse(true);
        var hasSecret = !executionStage.secretsByEnvVariable().isEmpty();

        if (hasInput || hasOutput) {
            pvc = persistentVolumeClaim(stageName, storageSizeGi, namespace);
            Volume inputVolume = new VolumeBuilder().withName(stageName).withNewPersistentVolumeClaim(stageName, false).build();
            volumes.add(inputVolume);
        }

        if (hasInput) {
            List<Container> containers = new ArrayList<>();
            for (final String inputStage : stage.inputStages()) {
                containers.add(storageProvider.initStorageContainer(executionStage.bucketName(), inputStage, stageName));
            }

            mounts.add(new VolumeMountBuilder().withName(stageName).withMountPath("/in").build());
            inputJob = job(KubernetesUtil.toValidRFC1123Label(stageName, "in"), containers, volumes, namespace, stageCopyServiceAccount);
        }

        if (hasSecret) {
            String secretName = KubernetesUtil.toValidRFC1123Label(stageName);
            secret = new SecretBuilder().withNewMetadata()
                    .withName(secretName)
                    .withNamespace(namespace)
                    .endMetadata()
                    .withStringData(executionStage.secretsByEnvVariable())
                    .build();

            for (var envVariable : executionStage.secretsByEnvVariable().keySet()) {
                var envVar = new EnvVarBuilder().withName(envVariable)
                        .withValueFrom(new EnvVarSourceBuilder().withNewSecretKeyRef(envVariable, secretName, false).build())
                        .build();
                environmentVariables.add(envVar);
            }
        }

        if (hasOutput) {
            mounts.add(new VolumeMountBuilder().withName(stageName).withMountPath("/out").withSubPath(stage.name()).build());
            onCompleteCopyJob = job(KubernetesUtil.toValidRFC1123Label(stageName, "out"),
                    List.of(storageProvider.exitStorageContainer(executionStage.bucketName(), stage.name(), stageName)),
                    volumes,
                    namespace,
                    stageCopyServiceAccount);
        }

        var containerBuilder =
                new ContainerBuilder().withName(stageName).withImage(imageName).withVolumeMounts(mounts).withEnv(environmentVariables);
        args.ifPresent(containerBuilder::withArgs);
        command.ifPresent(containerBuilder::withCommand);
        var container = containerBuilder.build();
        var runnerSa = executionStage.stage().options().flatMap(StageOptions::serviceAccount).orElse(null);
        job = job(stageName, List.of(container), volumes, namespace, runnerSa);
    }

    public String getStageName() {
        return stageName;
    }

    public StageRun createStageRun(BlockingKubernetesClient client) {
        return new OutputStageRun(pvc, job, inputJob, onCompleteCopyJob, secret, client);
    }

    @Override
    public String toString() {
        return Stream.of(pvc, secret, inputJob, job, onCompleteCopyJob)
                .filter(Objects::nonNull)
                .map(Serialization::asYaml)
                .collect(Collectors.joining());
    }

    private static PersistentVolumeClaim persistentVolumeClaim(String pvcName, int storageSizeGi, String namespace) {
        var pvcSpec = new PersistentVolumeClaimSpecBuilder().withAccessModes("ReadWriteOnce")
                .withStorageClassName("standard")
                .withNewResources()
                .addToRequests("storage", new Quantity(storageSizeGi + "Gi"))
                .endResources()
                .build();
        return new PersistentVolumeClaimBuilder().withNewMetadata()
                .withName(pvcName)
                .withNamespace(namespace)
                .endMetadata()
                .withSpec(pvcSpec)
                .build();
    }

    private static Job job(String jobName, List<Container> containers, List<Volume> volumes, String namespace, String serviceAccountName) {
        var pod = new PodSpecBuilder().withServiceAccountName(serviceAccountName)
                .withContainers(containers)
                .withRestartPolicy("Never")
                .withVolumes(volumes)
                .build();
        return new JobBuilder().withNewMetadata()
                .withName(jobName)
                .withNamespace(namespace)
                .endMetadata()
                .withSpec(new JobSpecBuilder().withBackoffLimit(1).withNewTemplate().withSpec(pod).endTemplate().build())
                .build();
    }
}

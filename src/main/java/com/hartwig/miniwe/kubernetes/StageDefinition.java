package com.hartwig.miniwe.kubernetes;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.miniwe.workflow.ExecutionStage;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimSpecBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
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
    private final PersistentVolumeClaim outputPvc;
    private final Job job;
    private final Job onCompleteCopyJob;

    public StageDefinition(ExecutionStage executionStage, String namespace, int storageSizeGi, String serviceAccountName,
            StorageProvider storageProvider) {
        this.stageName = executionStage.getName();
        var stage = executionStage.stage();
        var imageName = String.format("%s:%s", stage.image(), stage.version());

        var args = stage.arguments().map(arguments -> List.of(arguments.split(" ")));
        var command = stage.command().map(a -> List.of(a.split(" ")));

        List<Volume> volumes = new ArrayList<>();
        List<VolumeMount> mounts = new ArrayList<>();
        List<Container> initContainers = new ArrayList<>();
        for (final String inputStage : stage.inputStages()) {
            var volumeName = KubernetesUtil.toValidRFC1123Label(executionStage.runName(), inputStage);

            volumes.add(new VolumeBuilder().withName(volumeName).withNewEmptyDir().and().build());
            mounts.add(new VolumeMountBuilder().withName(volumeName).withMountPath("/in/" + inputStage).build());
            initContainers.add(storageProvider.initStorageContainer(executionStage.bucketName(), inputStage, volumeName));
        }

        String outputVolumeName = KubernetesUtil.toValidRFC1123Label(stageName);

        outputPvc = persistentVolumeClaim(outputVolumeName, storageSizeGi, namespace);
        var outputVolume = new VolumeBuilder().withName(outputVolumeName).withNewPersistentVolumeClaim(outputVolumeName, false).build();
        volumes.add(outputVolume);
        mounts.add(new VolumeMountBuilder().withName(outputVolumeName).withMountPath("/out").build());

        var containerBuilder = new ContainerBuilder().withName(stageName).withImage(imageName).withVolumeMounts(mounts);
        args.ifPresent(containerBuilder::withArgs);
        command.ifPresent(containerBuilder::withCommand);
        var container = containerBuilder.build();
        var pod = new PodSpecBuilder().withServiceAccountName(serviceAccountName)
                .withInitContainers(initContainers)
                .withContainers(container)
                .withRestartPolicy("Never")
                .withVolumes(volumes)
                .build();

        var jobSpec = new JobSpecBuilder().withBackoffLimit(1).withNewTemplate().withSpec(pod).endTemplate().build();
        job = new JobBuilder().withNewMetadata().withName(stageName).withNamespace(namespace).endMetadata().withSpec(jobSpec).build();

        // create on complete copy job
        var onCompleteCopyPod = new PodSpecBuilder().withServiceAccountName(serviceAccountName)
                .withContainers(storageProvider.exitStorageContainer(executionStage.bucketName(), stage.name(), outputVolumeName))
                .withRestartPolicy("Never")
                .withVolumes(outputVolume)
                .build();

        var onCompleteCopySpec =
                new JobSpecBuilder().withBackoffLimit(1).withNewTemplate().withSpec(onCompleteCopyPod).endTemplate().build();
        onCompleteCopyJob = new JobBuilder().withNewMetadata()
                .withName(KubernetesUtil.toValidRFC1123Label(stageName, "cp"))
                .withNamespace(namespace)
                .endMetadata()
                .withSpec(onCompleteCopySpec)
                .build();
    }

    public String getStageName() {
        return stageName;
    }

    public StageRun createStageRun(BlockingKubernetesClient client) {
        return new StageRun(outputPvc, job, onCompleteCopyJob, client);
    }

    @Override
    public String toString() {
        return Stream.of(outputPvc, job, onCompleteCopyJob).map(Serialization::asYaml).collect(Collectors.joining());
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
}

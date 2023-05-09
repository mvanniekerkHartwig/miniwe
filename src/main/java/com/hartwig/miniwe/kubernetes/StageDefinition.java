package com.hartwig.miniwe.kubernetes;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.miniwe.miniwdl.Stage;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.LifecycleBuilder;
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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.utils.Serialization;

public class StageDefinition {

    private final String namespace;
    private final String stageName;
    private final Job job;

    public StageDefinition(Stage stage, String runName, String workflowName, String namespace, int storageSizeGi, String serviceAccountName,
            StorageProvider storageProvider) {
        this.namespace = namespace;
        var imageName = String.format("%s:%s", stage.image(), stage.version());

        this.stageName = KubernetesUtil.toValidRFC1123Label(workflowName, runName, stage.name());

        var args = stage.arguments().map(arguments -> List.of(arguments.split(" ")));
        var entrypoint = stage.entrypoint().map(a -> List.of(a.split(" ")));

        List<Volume> volumes = new ArrayList<>();
        List<VolumeMount> mounts = new ArrayList<>();
        List<Container> initContainers = new ArrayList<>();
        for (final String inputStage : stage.inputStages()) {
            var volumeName = KubernetesUtil.toValidRFC1123Label(workflowName, runName, inputStage, "volume");

            volumes.add(new VolumeBuilder().withName(volumeName).withNewEmptyDir().and().build());
            mounts.add(new VolumeMountBuilder().withName(volumeName).withMountPath("/in/" + inputStage).build());
            initContainers.add(storageProvider.initStorageContainer(inputStage, volumeName));
        }

        var outputVolumeName = KubernetesUtil.toValidRFC1123Label(stageName, "volume");

        volumes.add(new VolumeBuilder().withName(outputVolumeName).withNewEmptyDir().and().build());
        mounts.add(new VolumeMountBuilder().withName(outputVolumeName).withMountPath("/out").build());
        var outputSidecar = storageProvider.exitStorageContainer(stageName, outputVolumeName);

        var lifecycle = new LifecycleBuilder().withNewPreStop()
                .withNewExec()
                .withCommand(List.of("sh", "-c", "touch /out/ready"))
                .endExec()
                .endPreStop()
                .withNewPostStart()
                .withNewExec()
                .withCommand(List.of("sh", "-c", "touch /out/started"))
                .endExec()
                .endPostStart()
                .build();
        var containerBuilder =
                new ContainerBuilder().withName(stageName).withImage(imageName).withVolumeMounts(mounts).withLifecycle(lifecycle);
        args.ifPresent(containerBuilder::withArgs);
        entrypoint.ifPresent(containerBuilder::withCommand);
        var container = containerBuilder.build();
        var pod = new PodSpecBuilder().withServiceAccountName(serviceAccountName)
                .withInitContainers(initContainers)
                .withContainers(container, outputSidecar)
                .withRestartPolicy("Never")
                .withVolumes(volumes)
                .build();

        var jobSpec = new JobSpecBuilder().withBackoffLimit(1).withNewTemplate().withSpec(pod).endTemplate().build();
        job = new JobBuilder().withNewMetadata().withName(stageName).withNamespace(namespace).endMetadata().withSpec(jobSpec).build();
    }

    public String getStageName() {
        return stageName;
    }

    public StageRun submit(KubernetesClient client) {
        return new StageRun(job, namespace, client);
    }

    @Override
    public String toString() {
        return Serialization.asYaml(job);
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

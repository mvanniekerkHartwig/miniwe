package com.hartwig.miniwe.kubernetes;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.hartwig.miniwe.miniwdl.Stage;

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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.utils.Serialization;

public class StageDefinition {

    private final String namespace;
    private final String stageName;
    private final PersistentVolumeClaim outputPvc;
    private final Job job;
    private final String serviceAccountName;

    public StageDefinition(Stage stage, String runName, String workflowName, String namespace, int storageSizeGi, String serviceAccountName) {
        this.serviceAccountName = serviceAccountName;
        this.namespace = namespace;
        var imageName = String.format("%s:%s", stage.image(), stage.version());

        this.stageName = KubernetesUtil.toValidRFC1123Label(workflowName, runName, stage.name());

        var args = stage.arguments().map(arguments -> List.of(arguments.split(" ")));
        var entrypoint = stage.entrypoint().map(a -> List.of(a.split(" ")));

        List<Volume> volumes = new ArrayList<>();
        List<VolumeMount> mounts = new ArrayList<>();
        for (final String inputStage : stage.inputStages()) {
            var volumeName = KubernetesUtil.toValidRFC1123Label(workflowName, runName, inputStage, "volume");
            var volumeMountPath = "/in/" + inputStage;

            volumes.add(new VolumeBuilder().withName(volumeName).withNewPersistentVolumeClaim(volumeName, true).build());
            mounts.add(new VolumeMountBuilder().withName(volumeName).withMountPath(volumeMountPath).build());
        }

        var outputVolumeName = KubernetesUtil.toValidRFC1123Label(stageName, "volume");

        outputPvc = persistentVolumeClaim(outputVolumeName, storageSizeGi, namespace);
        volumes.add(new VolumeBuilder().withName(outputVolumeName).withNewPersistentVolumeClaim(outputVolumeName, false).build());
        mounts.add(new VolumeMountBuilder().withName(outputVolumeName).withMountPath("/out").build());

        var containerBuilder = new ContainerBuilder().withName(stageName).withImage(imageName).withVolumeMounts(mounts);
        args.ifPresent(containerBuilder::withArgs);
        entrypoint.ifPresent(containerBuilder::withCommand);
        var container = containerBuilder.build();
        var pod = new PodSpecBuilder().withServiceAccountName(serviceAccountName)
                .withContainers(container)
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
        return new StageRun(outputPvc, job, namespace, client);
    }

    @Override
    public String toString() {
        return Stream.of(outputPvc, job).map(Serialization::asYaml).collect(Collectors.joining());
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

## Mini Workflow Engine

The Mini Workflow Engine (miniWE) is a simple task scheduler that runs in kubernetes.
miniWE supports running multiple workflows at the same time, and having multiple concurrent runs per workflow.
Run outputs are stored in "run-buckets" in GCP Cloud storage. miniWE supports running with existing run-buckets, skipping steps if the
output is already available.
miniWE is meant to be executed as a library as part of a persistent server. It also supports running in standalone mode from the command
line, this is the mode that is run when running the jar.

### Docker container structure

Docker containers that are run with miniWE are expected to have a certain structure:

- miniWE runs docker containers that take (optionally) input data and produce (optionally) output data.
- Previous stage outputs that the current stage depends on are mounted to the docker container as volumes at `/in/{stage_name}`.
- The current stage should write its output files to a volume mounted at `/out`.

### Workflow Definition

A workflow is described using a Workflow Definition. This takes the form of a YAML file with the following fields.

- name: The name of a workflow
- version: The version of a workflow. miniWE supports having multiple workflows with the same name if the version number is different.
- params: List with input parameters of a workflow. The values for these input parameters are defined in the "execution definition".
- stages: All stages of a workflow.
    - name: The name of a stage. This name is used as part of the kubernetes job name, and is used in subsequent stages to refer to as an
      input stage.
    - image: The docker image name
    - version: The docker image tag
    - entrypoint: Optional parameter to specify the "entrypoint" of a container (the command that the container will run).
    - arguments: Optional parameter to specify "arguments" for a container entrypoint command (It is expected that most docker images will
      define an entrypoint as part of the image).
    - inputStages: Optional list specifying all stages that the current stage depends on. These inputStages influence the order of execution
      of the containers, and outputs of previous stages will be mounted as inputs of the container.

Example workflow definition:

```yaml
name: "reporting-pipeline"
version: "1.0.0-alpha.1"
params:
  - patient_id
  - primary_tumor_doids
  - orange_path
  - output_path
stages:
  - name: orange
    image: "eu.gcr.io/hmf-build/google/cloud-sdk"
    version: "425.0.0"
    entrypoint: "gsutil rsync ${orange_path} /out"
  - name: rose
    image: "eu.gcr.io/hmf-pipeline-prod-e45b00f2/rose"
    version: "latest"
    arguments: "-patient_id ${patient_id}"
    inputStages:
      - orange
  - name: protect
    image: "eu.gcr.io/hmf-pipeline-prod-e45b00f2/protect"
    version: "latest"
    arguments: "-primary_tumor_doids ${primary_tumor_doids}"
    inputStages:
      - orange
  - name: output
    image: "eu.gcr.io/hmf-build/google/cloud-sdk"
    version: "425.0.0"
    entrypoint: "gsutil rsync /in ${output_path}"
    inputStages:
      - rose
      - protect
```

### Execution Definition

A workflow can be executed by an execution definition. The execution definition name must be unique per workload. If the execution
definition name corresponds to an existing run bucket in Cloud Storage, each stage for which output already exists in the run bucket will be
skipped. The execution definition is a YAML file with the following fields:

- name: Execution definition name
- workflow: Name of the workflow
- version: Version of the workflow
- params: List of key-value pairs where the key corresponds with the reporting pipeline params.

Example execution definition for the workflow above:

```yaml
name: "test-run"
workflow: "reporting-pipeline"
version: "1.0.0-alpha.1"
params:
  patient_id: "pid"
  primary_tumor_doids: "162"
  orange_path: "gs://reporting-pipeline-test-mvn/in/"
  output_path: "gs://reporting-pipeline-test-mvn/out/"
```

### Cloud Storage run buckets

Each execution definition corresponds to one unique run bucket.
The run bucket name takes the structure: `run-{{workflow-name}}-{{workflow-version}}-{{execution-name}}`. So for the execution above the
name of the run bucket will be `reporting-pipeline-1-0-0-alpha-1-test-run`.

Keeping the run bucket and rerunning an execution means that each stage for which a top-level directory already exists will be skipped.
Suppose the run bucket above already exists, and there is a top-level directory in the bucket called "orange". In that case the stage orange
will be skipped.

### Kubernetes execution

Each stage will be scheduled in kubernetes as a job. Each job will schedule two pods. The first pod will have one init container per input
stage (to copy the output of the run bucket for the stage) and one container to do the work. After the container that does the work
finishes, one more pod is created to copy the output of the work container to the cloud storage run bucket. If the pods succeed the
resources for the stage are cleaned up. If the pods fail the job is kept around for manual inspection.

##### Inspecting and cleaning up failed resources.

Suppose that a stage fails. We can find the failed stage with:

```sh
kc get pod | grep reporting-pipeline-1-0-0-alpha-1-test-run
```

We can inspect the logs with `kc logs {{pod-name}}`. If we are done inspecting we can grep the job and delete it like so:

```sh
kc get job | grep reporting-pipeline-1-0-0-alpha-1-test-run
kc delete job {{job-name}}
kc get pvc | grep reporting-pipeline-1-0-0-alpha-1-test-run
kc delete pvc {{pvc-name}}
```

This will also clean up the pods.

### Running the README.md workflow for testing

Compile and package:

```sh
mvn clean package
```

Make sure you are connected to the `hmf-pipeline-development` kubernetes cluster.

```sh
kubectl config get-contexts
```

Then run:

```sh
java -jar target/miniwe.jar \
src/test/resources/real-workflow.yaml \
src/test/resources/real-execution.yaml \
--gcp-project-id=hmf-pipeline-development \
--gcp-region=europe-west4 \
--kubernetes-namespace=pilot-1 \
--service-account-name=pipeline-launcher-sa 
```
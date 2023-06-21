## Mini Workflow Engine
- Mini workflow engine that runs in kubernetes.
- The workflow engine runs docker containers that specify entrypoints.
- Previous stage outputs that the current stage depends on are mounted to the docker container as volumes at `/in/{stage_name}`.
- The current stage should write its output files to a volume mounted at `/out`.

Example workflow definition:
```yaml
name: reporting-pipeline
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

Example execution definition for the workflow above:
```yaml
name: "test-run-mvn"
params:
  patient_id: "pid"
  primary_tumor_doids: "162"
  orange_path: "gs://run-colo829v003r-colo829v003t-ned-lcm-testing/orange/results/"
  output_path: "gs://test-run-mvn/"
```
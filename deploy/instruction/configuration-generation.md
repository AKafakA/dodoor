
This guide explains how to generate host/IP configurations based on your testbed and how to produce experiment configs used by the services and scripts. Dodoor currently runs without a simulator and does not include late-binding in the default docs.

---

Host/IP Configuration

This process generates the necessary host and IP configuration files based on your hardware setup.

1.  **Update Hardware Information**: Add your new host's hardware details to `deploy/resources/configuration/host_config_template.json`.

2.  **Add testbed Manifest**: Download the `manifest.xml` file from your testbed experiment and place it in `deploy/resources/configuration/manifest.xml`.

3.  **Generate Host Files**: Run the following script to parse the manifest and create the configuration files (set your CloudLab username via `CLOUDLAB_USER` to avoid hard-coding usernames in the generated SSH host lists):

    ```bash
    CLOUDLAB_USER=<your_cloudlab_username> python deploy/python/scripts/cl_manifest_parser.py
    ```

    This will generate the host and IP configuration files under `deploy/resources/host_addresses/cloud_lab/`.

The basic host setting is defined in `deploy/resources/configuration/host_config_template.json`, which includes host name, IP, node type, and number of slots. Update this file to match your hardware (e.g., mix of node/xl170/c6525-25g/c6620) and desired per-host concurrency.

---
Experiment Configuration Generation

The experiment configuration contains the tunable parameters for each experiment (scheduler, beta, batch size, weights, etc.). Generate it with:

```
python deploy/python/scripts/config_generator.py
```

Key flags you may care about:
- `--scheduler-type`: `dodoor` | `powerOfTwo` | `prequal` | `random`
- `--beta`, `--batch-size`, `--scheduler-num-tasks-update`
- `--cpu_weight`, `--memory_weight`, `--disk_weight`, `--duration_weight`
- `--network_interface` for binding

Note: late-binding (Sparrow) is not part of the default instructions due to open issues.

This generation step is also called by the end-to-end scripts; see `deploy/instruction/experiment.md`.


---
Task Configuration Generation

This process is for profiling new functions or node types to determine their resource requirements and execution times.

### (Optional) Adding New Tasks

If you're introducing new tasks, follow these steps first:

* Add new task scripts to `deploy/python/function_bench`.
* Place any required data in `deploy/python/function_bench/workload_data`.
* Update `deploy/python/requirements.txt` with any new Python dependencies.
* Define the new tasks in `deploy/resources/configuration/function_bench_config.json`.
* Commit these changes to your repository.

### Step 1: Initial Environment Setup & Profiling

First, profile the tasks on a host without resource limits. For example, on a testbed `xl170` node:

1.  **SSH into the host and set up the environment**:

    ```bash
    ssh username@host.example.testbed.us
    git clone <ANON_REPO_URL>
    cd dodoor
    pip install -r deploy/python/requirements.txt
    sh setup_docker.sh
    ```

2.  **Run the profiler**: This script profiles the tasks and stores the results in `deploy/python/function_bench/config`.

    ```bash
    python deploy/python/function_bench/task_profiler.py --iterations 100 --instance-id xl170
    ```

### Step 2: Merge Initial Profiles & Define Resource Slots

Next, transfer the profiled configs to your local machine to merge them. Here, you also define the maximum number of tasks (slots) that can run concurrently on a single host.

1.  **Copy the profiled configs to your local machine**:

    ```bash
    scp username@hp079.utah.testbed.us:~/dodoor/python/function_bench/config/unbox* ~/dodoor/deploy/resources/configuration/profiled_task_config/.
    ```

2.  **Merge the profiles**: This command merges the profiler outputs and adjusts the resource allocation based on the number of slots. In this example, we assume a maximum of **2** slots per host.

    ```bash
    python deploy/python/scripts/profiler_merge.py --override-num-slots-per-host 2
    ```

3.  **Upload the merged configuration back to the host**:

    ```bash
    scp deploy/resources/configuration/generated_config/merged_profiler_config.json username@hp079.utah.testbed.us:~/dodoor/deploy/python/function_bench/config/.
    ```

### Step 3: Profile Tasks in Docker with Resource Limits

Now, run a second profiler to estimate task durations within a Docker container under resource limits.

1.  **SSH into the host and run the Docker profiler**:

    ```bash
    ssh username@hp079.utah.testbed.us
    cd dodoor
    python deploy/python/function_bench/task_profiler_docker.py --iterations 100 --instance-id xl170
    ```

2.  **Clean up old profiles on your local machine**:

    ```bash
    rm ~/dodoor/deploy/resources/configuration/profiled_task_config/*
    ```

3.  **Copy the new Docker-based profiles to your local machine**:

    ```bash
    scp username@hp079.utah.testbed.us:~/dodoor/python/function_bench/config/docker* ~/dodoor/deploy/resources/configuration/profiled_task_config/.
    ```

### Step 4: Final Merge

Finally, merge the Docker-based profiling results to generate the final configuration file.

1.  **Run the final merge script**:

    ```bash
    python deploy/python/scripts/profiler_merge.py --simple-merge
    ```

2.  **Done!** The final configuration will be at `dodoor/deploy/resources/configuration/generated/merged_profiler_config.json`.

---

Service Startup (Manual)

Most experiments use the provided scripts to start services. If you prefer to start them manually with `ServiceDaemon`:

- Node on a worker:
  ```bash
  java -cp target/dodoor-1.0-SNAPSHOT.jar \
    org.anon.scheduler.ServiceDaemon \
    -c deploy/resources/configuration/example_dodoor_configuration.conf \
    -hc deploy/resources/host_addresses/cloud_lab/host_config.json \
    -tc deploy/resources/configuration/generated_config/merged_profiler_config.json \
    -d false -s false -n true
  ```
- Scheduler + Data Store on control node:
  ```bash
  java -cp target/dodoor-1.0-SNAPSHOT.jar \
    org.anon.scheduler.ServiceDaemon \
    -c deploy/resources/configuration/example_dodoor_configuration.conf \
    -hc deploy/resources/host_addresses/cloud_lab/host_config.json \
    -tc deploy/resources/configuration/generated_config/merged_profiler_config.json \
    -d true -s true -n false
  ```

Tip: Some helper scripts use a placeholder username (e.g., `asdwb`). Replace with your own where needed.

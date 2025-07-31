
  

This process generates the necessary host and IP configuration files based on your hardware setup.

1.  **Update Hardware Information**: Add your new host's hardware details to `deploy/resources/configuration/host_config_template.json`.

2.  **Add Cloudlab Manifest**: Download the `manifest.xml` file from your Cloudlab experiment and place it in `deploy/resources/configuration/manifest.xml`.

3.  **Generate Host Files**: Run the following script to parse the manifest and create the configuration files:

    ```bash
    python deploy/python/scripts/cl_manifest_parser.py
    ```

    This will generate the host and IP configuration files under `deploy/resources/host_addresses/cloud_lab/`.

The basic host setting is defined in `deploy/resources/configuration/host_config_template.json`, which includes the host name, IP address, and other hardware specifications and number of testing slots

---
## Experiment Configuration Generation

The experiment configuration contains the tunable parameters for each experiment, such as  scheduler, beta abd batch size can be generated using the following script:

```
python deploy/python/scripts/config_generator.py
```
However, such configuration generation is part of sub-experiments and auto included in the testing scripts, discussed under instruction/experiment.md.


---

## Task Configuration Generation

This process is for profiling new functions or node types to determine their resource requirements and execution times.

### (Optional) Adding New Tasks

If you're introducing new tasks, follow these steps first:

* Add new task scripts to `deploy/python/function_bench`.
* Place any required data in `deploy/python/function_bench/workload_data`.
* Update `deploy/python/requirements.txt` with any new Python dependencies.
* Define the new tasks in `deploy/resources/configuration/function_bench_config.json`.
* Commit these changes to your repository.

### Step 1: Initial Environment Setup & Profiling

First, we'll profile the tasks on a new host without any resource limitations. For this example, we'll use an `xl170` node in Cloudlab.

1.  **SSH into the host and set up the environment**:

    ```bash
    ssh username@hp079.utah.cloudlab.us
    git clone [https://github.com/AKafakA/dodoor.git](https://github.com/AKafakA/dodoor.git)
    cd dodoor
    pip install -r deploy/python/requirements.txt
    sh setup_docker.sh
    ```

2.  **Run the profiler**: This script profiles the tasks and stores the results in `deploy/python/function_bench/config`.

    ```bash
    python deploy/python/function_bench/task_profiler.py --iterations 100 --instance-id xl170
    ```

### Step 2: Merge Initial Profiles & Define Resource Slots

Next, transfer the profiled configurations to your local machine to merge them. Here, you'll also define the maximum number of tasks (slots) that can run concurrently on a single host.

1.  **Copy the profiled configs to your local machine**:

    ```bash
    scp username@hp079.utah.cloudlab.us:~/dodoor/python/function_bench/config/unbox* ~/dodoor/deploy/resources/configuration/profiled_task_config/.
    ```

2.  **Merge the profiles**: This command merges the profiler outputs and adjusts the resource allocation based on the number of slots. In this example, we assume a maximum of **2** slots per host.

    ```bash
    python deploy/python/scripts/profiler_merge.py --override-num-slots-per-host 2
    ```

3.  **Upload the merged configuration back to the host**:

    ```bash
    scp deploy/resources/configuration/generated_config/merged_profiler_config.json username@hp079.utah.cloudlab.us:~/dodoor/deploy/python/function_bench/config/.
    ```

### Step 3: Profile Tasks in Docker with Resource Limits

Now, we'll run a second profiler to estimate the task durations within a Docker container that has resource restrictions.

1.  **SSH into the host and run the Docker profiler**:

    ```bash
    ssh username@hp079.utah.cloudlab.us
    cd dodoor
    python deploy/python/function_bench/task_profiler_docker.py --iterations 100 --instance-id xl170
    ```

2.  **Clean up old profiles on your local machine**:

    ```bash
    rm ~/dodoor/deploy/resources/configuration/profiled_task_config/*
    ```

3.  **Copy the new Docker-based profiles to your local machine**:

    ```bash
    scp username@hp079.utah.cloudlab.us:~/dodoor/python/function_bench/config/docker* ~/dodoor/deploy/resources/configuration/profiled_task_config/.
    ```

### Step 4: Final Merge

Finally, merge the Docker-based profiling results to generate the final configuration file.

1.  **Run the final merge script**:

    ```bash
    python deploy/python/scripts/profiler_merge.py --simple-merge
    ```

2.  **Done!** The final, complete configuration file will be located at `dodoor/deploy/resources/configuration/generated/merged_profiler_config.json`.
````
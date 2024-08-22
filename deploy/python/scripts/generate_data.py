from deploy.python.data.generator.azure.azure_data_generator import AzureDataGenerator
from deploy.python.data.generator.gcp.google_cloud_task_data_generator import GoogleCloudTaskDataGenerator
from deploy.python.data.generator.gcp.google_cloud_v2_task_data_generator import GoogleCloudV2TaskDataGenerator

generate_azure = False

if generate_azure:
    azure_data_path = "deploy/resources/data/raw_data/azure_trace.sqlite"
    azure_output_path_10m = "deploy/resources/data/azure_data_cloudlab_10m"
    azure_data_generator = AzureDataGenerator(azure_data_path, machine_ids=range(1, 40),
                                              max_cores=48,
                                              max_memory=384 * 1024)
    data_10m = azure_data_generator.generate(10000000, 0, 1000 * 60 * 10, [1 / 24, 1 / 8],
                                             time_shift=1.0,
                                             timeline_compress_ratio=1.0,
                                             max_cores=8, max_memory=60 * 1024, max_disk=-1)
    azure_data_generator.write_data_target_output(data_10m, azure_output_path_10m)
    azure_output_path_1m = "deploy/resources/data/azure_data_cloudlab_1m"
    data_1m = azure_data_generator.generate(10000000, 0, 1000 * 60, [1 / 24, 1 / 8],
                                            time_shift=1.0,
                                            timeline_compress_ratio=1.0,
                                            max_cores=8, max_memory=60 * 1024, max_disk=-1)
    azure_data_generator.write_data_target_output(data_1m, azure_output_path_1m)

    print("Azure Data generation completed")

generate_gcp = False

if generate_gcp:
    gcp_event_path_dir = "deploy/resources/data/raw_data/google_task_events"
    gcp_output_path_1m = "deploy/resources/data/gcp_data_cloudlab_1m"
    gcp_output_path_10m = "deploy/resources/data/gcp_data_cloudlab_10m"
    gcp_data_generator = GoogleCloudTaskDataGenerator(gcp_event_path_dir,
                                                      max_cores=48,
                                                      max_memory=384 * 1024)
    data_1m = gcp_data_generator.generate(10000000, 0, 1000 * 1, [0, 1 / 8],
                                          max_cores=8, max_memory=60 * 1024, max_disk=-1)
    gcp_data_generator.write_data_target_output(data_1m, gcp_output_path_1m)

    data_10m = gcp_data_generator.generate(10000000, 0, 1000 * 60 * 10, [0, 1 / 8],
                                           max_cores=8, max_memory=60 * 1024, max_disk=-1)
    gcp_data_generator.write_data_target_output(data_10m, gcp_output_path_10m)

generate_gcp_v2 = True

if generate_gcp_v2:
    gcp_event_path_dir = "deploy/resources/data/raw_data/google_cloud_v2/instance_events-000000000000.json"
    gcp_output_path_1m = "deploy/resources/data/gcp_v2_data_cloudlab_1m"
    gcp_output_path_10m = "deploy/resources/data/gcp_v2_data_cloudlab_10m"
    gcp_datav2_generator = GoogleCloudV2TaskDataGenerator(gcp_event_path_dir,
                                                          max_cores=48,
                                                          max_memory=384 * 1024)
    data_1m = gcp_datav2_generator.generate(10000000, 0, 1000 * 60 * 1, [0, 1 / 8],
                                            max_cores=8, max_memory=60 * 1024, max_disk=-1)
    gcp_datav2_generator.write_data_target_output(data_1m, gcp_output_path_1m)
    data_10m = gcp_datav2_generator.generate(10000000, 0, 1000 * 60 * 1000, [0, 1 / 8],
                                             max_cores=8, max_memory=60 * 1024, max_disk=-1)
    gcp_datav2_generator.write_data_target_output(data_10m, gcp_output_path_10m)

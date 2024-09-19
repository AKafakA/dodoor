from deploy.python.data.generator.azure.azure_data_generator import AzureDataGenerator
from deploy.python.data.generator.gcp.google_cloud_task_data_generator import GoogleCloudTaskDataGenerator
from deploy.python.data.generator.gcp.google_cloud_v2_task_data_generator import GoogleCloudV2TaskDataGenerator
import random

generate_azure = True
if generate_azure:
    # random pick one hour in the first day to collect the trace
    # time_window = 1 / 48
    # start_point = random.uniform(0, 1 - time_window)
    # end_point = start_point + time_window

    # pick 2 hour traces in the first day from 2:00 to 4:00
    time_window = 1 / 12
    start_point = 2 / 24
    end_point = start_point + time_window

    azure_data_path = "deploy/resources/data/raw_data/azure_trace.sqlite"
    azure_output_path_10m = "deploy/resources/data/azure_data_cloudlab_10m"
    print("tracking all trace between {} and {} (in days)".format(start_point, end_point))

    azure_data_generator = AzureDataGenerator(azure_data_path, machine_ids=range(1, 2),
                                              max_cores=48,
                                              max_memory=348 * 1024)
    data_10m = azure_data_generator.generate(10000000, 0, 1000 * 60 * 10,
                                             [start_point, end_point],
                                             time_shift=1.0,
                                             timeline_compress_ratio=1.0,
                                             max_cores=8, max_memory=62 * 1024, max_disk=-1)
    azure_data_generator.write_data_target_output(data_10m, azure_output_path_10m)
    azure_output_path_1m = "deploy/resources/data/azure_data_cloudlab_1m"
    data_1m = azure_data_generator.generate(10000000, 0, 1000 * 60,
                                            [start_point, end_point],
                                            time_shift=1.0,
                                            timeline_compress_ratio=1.0,
                                            max_cores=8, max_memory=62 * 1024, max_disk=-1)
    # azure_data_generator.write_data_target_output(data_1m, azure_output_path_1m)

    print("Azure Data generation completed")

generate_gcp = False

if generate_gcp:
    gcp_event_path_dir = "deploy/resources/data/raw_data/google_task_events"
    gcp_output_path_1m = "deploy/resources/data/gcp_data_cloudlab_1m"
    gcp_output_path_10m = "deploy/resources/data/gcp_data_cloudlab_10m"
    gcp_data_generator = GoogleCloudTaskDataGenerator(gcp_event_path_dir,
                                                      max_cores=48,
                                                      max_memory=384 * 1024)
    data_1m = gcp_data_generator.generate(10000000, 0, 1000 * 1, [0, 1 / 12],
                                          max_cores=8, max_memory=60 * 1024, max_disk=-1)
    gcp_data_generator.write_data_target_output(data_1m, gcp_output_path_1m)

    data_10m = gcp_data_generator.generate(10000000, 0, 1000 * 60 * 10, [0, 1 / 12],
                                           max_cores=8, max_memory=60 * 1024, max_disk=-1)
    gcp_data_generator.write_data_target_output(data_10m, gcp_output_path_10m)

generate_gcp_v2 = False

if generate_gcp_v2:
    gcp_event_path_dir = "deploy/resources/data/raw_data/google_cloud_v2/instance_events-000000000000.json"
    gcp_output_path_1m = "deploy/resources/data/gcp_v2_data_cloudlab_1m"
    gcp_output_path_10m = "deploy/resources/data/gcp_v2_data_cloudlab_10m"
    gcp_datav2_generator = GoogleCloudV2TaskDataGenerator(gcp_event_path_dir,
                                                          max_cores=48,
                                                          max_memory=384 * 1024)
    data_1m = gcp_datav2_generator.generate(10000000, 0, 1000 * 60 * 1, [0, 1 / 12],
                                            max_cores=8, max_memory=60 * 1024, max_disk=-1)
    gcp_datav2_generator.write_data_target_output(data_1m, gcp_output_path_1m)
    data_10m = gcp_datav2_generator.generate(10000000, 0, 1000 * 60 * 10, [0, 1 / 12],
                                             max_cores=8, max_memory=60 * 1024, max_disk=-1)
    gcp_datav2_generator.write_data_target_output(data_10m, gcp_output_path_10m)

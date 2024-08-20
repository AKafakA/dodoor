from deploy.python.data.generator.azure.azure_data_generator import AzureDataGenerator

data_path = "deploy/resources/data/raw_data/azure_trace.sqlite"
output_path = "deploy/resources/data/azure_data_cloudlab"

azure_data_generator = AzureDataGenerator(data_path, output_path, machine_ids=range(1, 40), max_cores=24,
                                          max_memory=256 * 1024)
data = azure_data_generator.generate(10000000, 0, 1000 * 60 * 10, [0, 1/12], time_shift=-1,
                                     timeline_compress_ratio=1.0,
                                     max_cores=8, max_memory=60 * 1024, max_disk=-1)
azure_data_generator.write_data(data)
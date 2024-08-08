from deploy.python.data.generator.azure.azure_data_generator import AzureDataGenerator

data_path = "deploy/resources/data/raw_data/azure_trace.sqlite"
output_path = "deploy/resources/data/azure_data"

azure_data_generator = AzureDataGenerator(data_path, output_path, machine_ids=range(0, 10), max_cores=8,
                                          max_memory=60 * 1024)
data = azure_data_generator.generate(1000000, 0, 1000 * 60 * 0.5, [0, 1/12], time_shift=1/12)
azure_data_generator.write_data(data)
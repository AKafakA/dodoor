from deploy.python.data.generator.azure.azure_data_generator import AzureDataGenerator

data_path = "deploy/resources/data/azure_trace.sqlite"
output_path = "deploy/resources/data/azure_data"

azure_data_generator = AzureDataGenerator(data_path, output_path, machine_ids=range(0, 10))
data = azure_data_generator.generate(1000000, 0, 60000, [0, 0.1])
azure_data_generator.write_data(data)

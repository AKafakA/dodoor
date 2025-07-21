import argparse

from deploy.python.data.generator.azure.azure_data_generator import AzureDataGenerator
from deploy.python.data.generator.function_bench.FunctionBenchGenerator import FunctionBenchGenerator
from deploy.python.data.generator.serverless.serverless_data_generator import ServerlessDataGenerator


def generate_serverless_data(serverless_data_dir,
                             serverless_output_path,
                             num_records,
                             qps,
                             distribution_type,
                             burstiness,
                             num_functions=200):
    serverlessDataGenerator = ServerlessDataGenerator(
        serverless_dat_dir=serverless_data_dir,
        target_cluster_qps=qps,
        distribution_type=distribution_type,
        burstiness=burstiness,
        num_functions=num_functions,
    )
    data = serverlessDataGenerator.generate(num_records=num_records, start_id=0)
    serverlessDataGenerator.write_data_target_output(data, serverless_output_path)


def generate_azure_data(azure_data_path,
                        azure_output_path,
                        num_records,
                        target_qps=-1,
                        distribution_type="gamma",
                        burstiness=1.0,
                        max_cores=8,
                        max_memory=62 * 1024,
                        max_duration=60 * 1000):

    azure_data_generator = AzureDataGenerator(azure_data_path, machine_ids=range(1, 36),
                                              max_cores=48,
                                              max_memory=62 * 1024,
                                              target_qps=target_qps,
                                              distribution_type=distribution_type,
                                              burstiness=burstiness)
    data = azure_data_generator.generate(num_records, 0, max_duration,
                                             [0, 14],
                                             max_cores=max_cores, max_memory=max_memory, max_disk=0)
    azure_data_generator.write_data_target_output(data, azure_output_path)


def generate_function_bench_trace(config_address,
                                  output_path,
                                  num_records,
                                  qps,
                                  distribution_type="gamma",
                                  burstiness=1.0,
                                  task_distribution=None):
    generator = FunctionBenchGenerator(config_address=config_address, target_cluster_qps=qps,
                                       task_distribution=task_distribution,
                                       distribution_type=distribution_type, burstiness=burstiness)

    data = generator.generate(num_records=num_records, start_id=0)
    generator.write_data_target_output(data, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target_qps", type=int, default=10,
                        help="Target QPS for the Function Bench trace")
    parser.add_argument("--distribution_type", type=str, default="gamma",
                        help="Distribution type for the Function Bench trace")
    parser.add_argument("--burstiness", type=float, default=1.0,
                        help="Burstiness factor for the Function Bench trace")
    parser.add_argument("--num_records", type=int, default=100,
                        help="Number of records to generate")
    parser.add_argument("--generated_dataset", nargs='+',
                        default=["azure", "function_bench", "serverless"], )
    parser.add_argument("--azure_data_path", type=str,
                        default="deploy/resources/data/trace_data/azure_trace.sqlite",
                        help="Path to Azure trace data")
    parser.add_argument("--azure_output_path", type=str,
                        default="deploy/resources/data/azure_data",
                        help="Path to save the processed Azure data for replaying")
    parser.add_argument("--max_cores", type=int, default=8,
                        help="Maximum number of cores for the generated records for smallest hosts, "
                             "Used by Azure to convert to real cores")
    parser.add_argument("--max_memory", type=int, default=62 * 1024,
                        help="Maximum memory for the generated records in MB for smallest hosts for "
                             "Used by Azure to convert to real memory")
    parser.add_argument("--max_duration", type=int, default=1000 * 60 * 1,
                        help="Maximum duration for the generated records in milliseconds for Azure, default is 1 minute")
    parser.add_argument("--function_bench_config", type=str,
                        default="deploy/resources/configuration/function_bench_config.json",
                        help="Path to Function Bench trace data")
    parser.add_argument("--function_bench_trace_output_path", type=str,
                        default="deploy/resources/data/function_bench_trace.csv",
                        help="Path to save the generated Function Bench trace")
    parser.add_argument("--serverless_data_dir", type=str,
                        default="deploy/resources/data/trace_data/huawei_serverless",
                        help="Directory containing serverless trace data")
    parser.add_argument("--serverless_output_path", type=str,
                        default="deploy/resources/data/serverless_trace.csv",
                        help="Path to save the generated serverless trace")
    parser.add_argument("--serverless_num_functions", type=int, default=200,
                        help="Number of functions to generate in the serverless trace")
    args = parser.parse_args()
    if "azure" in args.generated_dataset:
        assert args.azure_data_path is not None, "Azure data path must be provided"
        assert args.azure_output_path is not None, "Azure output path must be provided"
        # if not use target qps but the raw trace timeline, set the target_qps to -1
        generate_azure_data(
            azure_data_path=args.azure_data_path,
            azure_output_path=args.azure_output_path,
            num_records=args.num_records,
            target_qps=args.target_qps,
            distribution_type=args.distribution_type,
            burstiness=args.burstiness,
            max_cores=args.max_cores,
            max_memory=args.max_memory,
            max_duration=args.max_duration
        )

    if "function_bench" in args.generated_dataset:
        assert args.function_bench_config is not None, "Function Bench config path must be provided"
        generate_function_bench_trace(
            config_address=args.function_bench_config,
            output_path=args.function_bench_trace_output_path,
            num_records=args.num_records,
            qps=args.target_qps,
            distribution_type=args.distribution_type,
            burstiness=args.burstiness
        )

    if "serverless" in args.generated_dataset:
        serverless_data_dir = args.serverless_data_dir
        serverless_output_path = args.serverless_output_path
        generate_serverless_data(
            serverless_data_dir=serverless_data_dir,
            serverless_output_path=serverless_output_path,
            num_records=args.num_records,
            qps=args.target_qps,
            distribution_type=args.distribution_type,
            burstiness=args.burstiness,
            num_functions=args.serverless_num_functions
        )

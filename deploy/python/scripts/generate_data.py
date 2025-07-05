import argparse

from deploy.python.data.generator.azure.azure_data_generator import AzureDataGenerator
from deploy.python.data.generator.function_bench.FunctionBenchGenerator import FunctionBenchGenerator


def generate_azure_data(azure_data_path, azure_output_path, max_cores=28, max_memory=62 * 1024,
                        num_records=10000000, max_duration=1000 * 60 * 10):
    """
    # read all trace from azure data and generate a new trace with 10 million records
    :param azure_data_path: Path to the Azure trace data
    :param azure_output_path: Path to save the processed Azure data for replaying
    :param max_cores: Maximum number of cores for the generated records
    :param max_memory: Maximum memory for the generated records in MB
    :param num_records: Number of records to generate
    :param max_duration: Maximum duration for the generated records in milliseconds

    """

    # azure_data_path = "deploy/resources/data/raw_data/azure_trace.sqlite"
    # azure_output_path_10m = "deploy/resources/data/azure_data_cloudlab_10m"

    azure_data_generator = AzureDataGenerator(azure_data_path, machine_ids=range(1, 36),
                                              max_cores=48,
                                              max_memory=62 * 1024)
    data_10m = azure_data_generator.generate(10000, 0, max_duration,
                                             [0, 14],
                                             time_shift=1.0,
                                             timeline_compress_ratio=1.0,
                                             max_cores=max_cores, max_memory=max_memory, max_disk=-1)
    azure_data_generator.write_data_target_output(data_10m, azure_output_path)


def generate_function_bench_trace(num_records, qps, config_address, output_path,
                                  task_distribution=None, distribution_type="gamma", burstiness=1.0):

    generator = FunctionBenchGenerator(config_address=config_address, target_cluster_qps=qps,
                                       task_distribution=task_distribution,
                                       distribution_type=distribution_type, burstiness=burstiness)

    data = generator.generate(num_records=num_records, start_id=0)
    generator.write_data_target_output(data, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_records", type=int, default=1000,
                        help="Number of records to generate")
    parser.add_argument("--max_cores", type=int, default=28,
                        help="Maximum number of cores for the generated records")
    parser.add_argument("--max_memory", type=int, default=62 * 1024,
                        help="Maximum memory for the generated records in MB")
    parser.add_argument("--max_duration", type=int, default=1000 * 60 * 1,
                        help="Maximum duration for the generated records in milliseconds")
    parser.add_argument("--generated_dataset", nargs='+',
                        default=["azure", "function_bench"])
    parser.add_argument("--azure_data_path", type=str,
                        default="deploy/resources/data/trace_data/azure_trace.sqlite",
                        help="Path to Azure trace data")
    parser.add_argument("--azure_output_path", type=str,
                        default="deploy/resources/data/azure_data",
                        help="Path to save the processed Azure data for replaying")
    parser.add_argument("--function_bench_config", type=str,
                        default="deploy/resources/configuration/function_bench_config.json",
                        help="Path to Function Bench trace data")
    parser.add_argument("--target_qps", type=int, default=10000,
                        help="Target QPS for the Function Bench trace")
    args = parser.parse_args()
    if "azure" in args.generated_dataset:
        assert args.azure_data_path is not None, "Azure data path must be provided"
        assert args.azure_output_path is not None, "Azure output path must be provided"
        generate_azure_data(args.azure_data_path, args.azure_output_path, args.max_cores, args.max_memory,
                            args.num_records, args.max_duration)

    if "function_bench" in args.generated_dataset:
        cluster_qps = args.target_qps
        assert args.function_bench_config is not None, "Function Bench config path must be provided"
        generate_function_bench_trace(args.num_records, cluster_qps, args.function_bench_config,
                                      "deploy/resources/data/function_bench_trace.csv")

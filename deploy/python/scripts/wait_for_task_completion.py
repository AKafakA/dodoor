import argparse
import time
import sys
import subprocess


def inject_summary_to_log(args, service_out_log_path, service_metrics_log_path, target_log_line_prefix):
    """Appends a summary of the experiment to the remote log file via SSH."""

    try:
        # grep the target log line if service_out_log_path exists and copy it to service_metrics_log_path
        # do this 2 steps 1) first check if log exists and print it out in python 2) append it to the metrics log
        # do not need to check if the log exists, just grep it and if it does not exist, it will return empty
        ssh_command_to_fetch_log = (
            f"grep '{target_log_line_prefix}' {service_out_log_path}"
        )
        fetch_log_command = ["ssh", args.host, ssh_command_to_fetch_log]
        fetch_log_result = subprocess.run(
            fetch_log_command,
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        fetched_log = fetch_log_result.stdout.strip()
        if not fetched_log:
            print("\n⚠️  Warning: No log lines fetched from the remote service output log.")
            return
        print(f"\nFetched log line to inject:\n{fetched_log} with prefix '{target_log_line_prefix}'")
        # Now append the fetched log to the metrics log file
        ssh_command_to_append_log = (
            f"echo '{fetched_log}' >> {service_metrics_log_path}"
        )
        append_log_command = ["ssh", args.host, ssh_command_to_append_log]
        subprocess.run(
            append_log_command,
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        print(f"\n✅ Successfully injected summary into remote log at {service_metrics_log_path}.")

    except subprocess.CalledProcessError as e:
        print(f"\n⚠️  Warning: Could not inject summary into remote log.")
        print(f"   - Remote command failed with error: {e.stderr}")
    except Exception as e:
        print(f"\n⚠️  Warning: An unexpected error occurred while injecting summary.")
        print(f"   - Error: {e}")


def main():
    """Main function to parse arguments and monitor the remote log file."""
    parser = argparse.ArgumentParser(
        description="Wait for tasks to complete by monitoring a remote log file via SSH."
    )
    parser.add_argument(
        "--host",
        type=str,
        required=True,
        help="The remote server to connect to via SSH (e.g., 'user@server.ip')."
    )
    parser.add_argument(
        "--scheduler_type",
        type=str,
        required=True,
        help="The type of scheduler being monitored."
    )
    parser.add_argument(
        "--num_requests",
        type=int,
        required=True,
        help="The total number of requests (tasks) to wait for."
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Maximum time to wait in minutes before timing out. A value of 0 means no timeout."
    )
    parser.add_argument(
        "--check_interval_in_seconds",
        type=int,
        default=600,
        help="Interval in seconds to check the remote log file for updates. Default is 10 minutes (600 seconds)."
    )

    args = parser.parse_args()
    remote_log_path = f"~/{args.scheduler_type}_scheduler_metrics.log"
    remote_output_path = f"~/{args.scheduler_type}_scheduler_service.out"

    # The exact string to find in the remote log file.
    target_selected_task_log_line = "type=HISTOGRAM, name=scheduler.metrics.tasks.e2e.makespan.latency.histograms, count="
    target_finished_task_log_line = "type=COUNTER, name=scheduler.metrics.tasks.finished.count, count="
    target_throughput_log_line = "Finished all tracked tasks"

    print(f"🚀 Starting local monitoring of remote log on '{args.host}'.")
    print(f"   - Remote Log File: {remote_log_path}")
    print(f"   - Waiting for: {args.num_requests} tasks")
    print(f"   - Timeout: {'No timeout' if args.timeout <= 0 else f'{args.timeout} minutes'}")
    print("-" * 30)

    start_time = time.time()
    timeout_seconds = args.timeout * 60
    check_interval = args.check_interval_in_seconds

    max_retries = 2  # Maximum retries for SSH command

    try:
        while True:
            elapsed_time = time.time() - start_time
            # Construct the remote command to be executed via SSH.
            remote_command = f"grep '{target_selected_task_log_line}' {remote_log_path}"
            ssh_command = ["ssh", args.host, remote_command]

            try:
                result = subprocess.run(
                    ssh_command,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=30,
                )

                if result.returncode > 1:  # 0=found, 1=not found. >1 is an error.
                    print(f"\n❌ SSH or remote command error (Exit Code: {result.returncode}):")
                    print(f"   - Stderr: {result.stderr.strip()}")
                    max_retries -= 1
                    if max_retries <= 0:
                        print("   - Maximum retries exceeded. Exiting.")
                        sys.exit(1)

                selected_completed_count = 0
                if result.stdout:
                    counts = []
                    for line in result.stdout.strip().split('\n'):
                        try:
                            count = int([s for s in line.split(', ') if s.startswith('count=')][0].split('=')[1])
                            counts.append(count)
                        except (ValueError, IndexError):
                            continue
                    selected_completed_count = max(counts, default=0)

                print(
                    f"\r[{int(elapsed_time):>5}s] Last check found {selected_completed_count}/{args.num_requests} "
                    f"selected tasks completed.", end="")

                if selected_completed_count >= args.num_requests or (args.timeout > 0 and elapsed_time >= timeout_seconds):
                    total_time = time.time() - start_time
                    if selected_completed_count >= args.num_requests:
                        print(f"\n✅ Required number of tasks completed.")
                    else:
                        print(f"\n⏰ Timeout reached after {args.timeout} minutes.")

                    total_count_command = f"grep '{target_finished_task_log_line}' {remote_log_path}"
                    total_count_ssh_command = ["ssh", args.host, total_count_command]
                    total_count_result = subprocess.run(
                        total_count_ssh_command,
                        capture_output=True,
                        text=True,
                        check=False,
                        timeout=30,
                    )
                    if total_count_result.returncode != 0:
                        print(f"\n❌ Error retrieving total completed tasks count: {total_count_result.stderr.strip()}")
                        sys.exit(1)
                    try:
                        total_count_line = total_count_result.stdout.strip().split('\n')[-1]
                        completed_count = int(total_count_line.split("count=")[-1].strip())
                    except (ValueError, IndexError):
                        print("\n❌ Error parsing total completed tasks count from remote log.")
                        sys.exit(1)

                    # Call the function to inject the summary into the remote log.
                    inject_summary_to_log(args,
                                          service_out_log_path=remote_output_path,
                                          service_metrics_log_path=remote_log_path,
                                          target_log_line_prefix=target_throughput_log_line
                                          )
                    print(f"   - Total time: {total_time:.2f} seconds and completed {completed_count} requests."
                          f" and selected {selected_completed_count} requests.")
                    print("\n🛑 Monitoring stopped successfully.")
                    sys.exit(0)

            except FileNotFoundError:
                print("\n❌ Error: 'ssh' command not found. Is OpenSSH client installed and in your PATH?")
                sys.exit(1)
            except subprocess.TimeoutExpired:
                print("\n⚠️  SSH command timed out. Will retry in 1 minute.")
            except Exception as e:
                print(f"\nAn unexpected error occurred: {e}")
                sys.exit(1)

            time.sleep(check_interval)

    except KeyboardInterrupt:
        print("\n\n🛑 Monitoring stopped by user.")
        sys.exit(1)


if __name__ == "__main__":
    main()

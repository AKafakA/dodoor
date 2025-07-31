#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitors a log file to wait for a specific number of tasks to be marked
as finished, checking for the completion marker every minute.
"""

import argparse
import time
from pathlib import Path
import sys

def main():
    """Main function to parse arguments and monitor the log file."""
    parser = argparse.ArgumentParser(
        description="Wait for a specific number of tasks to complete by monitoring a log file."
    )
    parser.add_argument(
        "--log",
        type=Path,
        required=True,
        help="Path to the scheduler's log file to monitor."
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
        default=60 ,
        help="Maximum time to wait in minutes before timing out. Default: 60 minutes."
    )

    args = parser.parse_args()

    # Construct the exact string we need to find in the log file.
    target_log_line = (
        f"type=COUNTER, name=scheduler.metrics.tasks.finished.count, count="
    )

    if not args.log.is_file():
        print(f"❌ Error: The log file {args.log} does not exist or is not a file.")
        sys.exit(1)

    print(f"🔍 Monitoring log file: {args.log} at {time.strftime('%H:%M:%S')}")
    start_time = time.time()
    timeout_seconds = args.timeout * 60  # Convert minutes to seconds
    while True:
        try:
            # Open and read the log file to find the target line.
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout_seconds:
                print(f"\n❌ Timeout! Exceeded {args.timeout} minutes.")
                print("   The completion marker was not found in the log file.")
                sys.exit(1) # Exit with a non-zero status to indicate failure

            with args.log.open("r", encoding="utf-8", errors="ignore") as f:
                num_finished_count = []
                for line in f:
                    if target_log_line in line:
                        # Extract the count from the line
                        count_str = line.split("count=")[-1].strip()
                        count = int(count_str)
                        num_finished_count.append(count)
                if max(num_finished_count, default=0) >= args.num_requests:
                    print(f"\n✅ All {args.num_requests} tasks have been marked as finished in {elapsed_time} ms.")
                    sys.exit(0)

            # If the line was not found after reading the whole file:
            time.sleep(60)
        except KeyboardInterrupt:
            print("\n🛑 Monitoring stopped by user.")
            break
        except Exception as e:
            print(f"\nAn unexpected error occurred: {e}")
            break


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Phase 4: Comprehensive validation experiments using Azure traces.

This validation framework tests all 4 schedulers using real Azure workload data
and verifies expected performance rankings match physical experiments.
"""

import sys
import subprocess
import json
import time
import os
from pathlib import Path
from typing import Dict, List, Any

# Add simulator to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class AzureValidationFramework:
    """
    Comprehensive validation using Azure traces to verify scheduler performance rankings.

    Expected Performance Rankings (from physical experiments):
    1. Dodoor: BEST performance (cached scheduling, sophisticated load balancing)
    2. Prequal: Good performance (cached with probe pool management)
    3. Sparrow: Moderate performance (late-binding overhead)
    4. PowerOfTwo: WORST performance (runtime probing overhead)
    """

    def __init__(self, output_base_dir: str = "simulator/debug/validation"):
        self.output_base_dir = Path(output_base_dir)
        self.output_base_dir.mkdir(parents=True, exist_ok=True)

        # Find project root directory (where CLAUDE.md exists)
        self.project_root = Path(__file__).parent.parent.parent
        while not (self.project_root / "CLAUDE.md").exists() and self.project_root != self.project_root.parent:
            self.project_root = self.project_root.parent

        # Azure trace path relative to project root
        self.azure_trace_path = str(self.project_root / "deploy/resources/data/azure_data/test_data")

        # Test configurations matching physical experiments
        self.test_configs = {
            "quick_validation": {
                "description": "Fast 5-minute validation across all schedulers",
                "duration_ms": 300000,  # 5 minutes
                "qps_rates": [20.0],  # Single QPS for quick testing
                "schedulers": ["dodoor", "prequal", "sparrow", "power_of_two"],
                "azure_trace_path": self.azure_trace_path
            },

            "full_validation": {
                "description": "Complete validation matching debug.sh parameters",
                "duration_ms": 1800000,  # 30 minutes (like physical experiments)
                "target_completed_tasks": 30000,  # Like NUM_REQUESTS in debug.sh
                "qps_rates": [30.0, 20.0, 10.0],  # Multiple QPS like debug.sh
                "schedulers": ["dodoor", "prequal", "sparrow", "power_of_two"],
                "azure_trace_path": self.azure_trace_path
            },

            "performance_ranking": {
                "description": "Focused test to verify scheduler performance order",
                "duration_ms": 600000,  # 10 minutes
                "qps_rates": [25.0],  # High load to differentiate performance
                "schedulers": ["dodoor", "prequal", "sparrow", "power_of_two"],
                "azure_trace_path": self.azure_trace_path,
                "cluster_config": "heterogeneous_100_nodes"  # Large cluster like debug.sh
            }
        }

    def run_validation_experiment(self, config_name: str) -> Dict[str, Any]:
        """Run a specific validation experiment configuration."""

        if config_name not in self.test_configs:
            raise ValueError(f"Unknown config: {config_name}")

        config = self.test_configs[config_name]
        print(f"🚀 Running {config['description']}")
        print(f"   Duration: {config['duration_ms']/1000/60:.1f} minutes")
        print(f"   QPS rates: {config['qps_rates']}")
        print(f"   Schedulers: {config['schedulers']}")

        results = {}
        experiment_start = time.time()

        for qps in config['qps_rates']:
            print(f"\n📈 Testing QPS: {qps}")
            qps_results = {}

            for scheduler in config['schedulers']:
                print(f"  🔄 Testing {scheduler}...")

                # Create experiment-specific output directory
                output_dir = self.output_base_dir / config_name / f"qps_{qps}" / scheduler

                # Run single scheduler experiment with Azure traces
                result = self._run_single_experiment(
                    scheduler=scheduler,
                    qps=qps,
                    duration_ms=config['duration_ms'],
                    azure_trace_path=config['azure_trace_path'],
                    output_dir=output_dir,
                    target_tasks=config.get('target_completed_tasks'),
                    cluster_config=config.get('cluster_config', "default_4_nodes")
                )

                qps_results[scheduler] = result
                print(f"    ✅ {scheduler}: {result['tasks_completed']} tasks, "
                      f"{result['throughput']:.1f} tasks/sec")

            results[f"qps_{qps}"] = qps_results

        experiment_time = time.time() - experiment_start
        results['experiment_metadata'] = {
            'config_name': config_name,
            'total_time_seconds': experiment_time,
            'azure_trace_used': config['azure_trace_path']
        }

        # Validate performance rankings
        ranking_validation = self._validate_performance_rankings(results)
        results['ranking_validation'] = ranking_validation

        # Save results
        results_file = self.output_base_dir / f"{config_name}_results.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)

        print(f"\n✅ {config['description']} completed in {experiment_time:.1f}s")
        print(f"📊 Results saved: {results_file}")

        return results

    def _run_single_experiment(self, scheduler: str, qps: float, duration_ms: int,
                              azure_trace_path: str, output_dir: Path,
                              target_tasks: int = None, cluster_config: str = "default_4_nodes") -> Dict[str, Any]:
        """Run a single scheduler experiment with Azure traces."""

        # Create Azure trace configuration
        config_file = self._create_azure_config(
            scheduler=scheduler,
            qps=qps,
            duration_ms=duration_ms,
            azure_trace_path=azure_trace_path,
            target_tasks=target_tasks,
            cluster_config=cluster_config
        )

        # Run simulation
        start_time = time.time()
        try:
            cmd = [
                "python", "simulator/run_simulation.py",
                "--config", str(config_file),
                "--output-dir", str(output_dir)
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, cwd=".", timeout=300)

            if result.returncode != 0:
                print(f"    ❌ {scheduler} experiment failed: {result.stderr}")
                return {"error": result.stderr, "tasks_completed": 0, "throughput": 0.0}

        except subprocess.TimeoutExpired:
            print(f"    ⏰ {scheduler} experiment timed out")
            return {"error": "timeout", "tasks_completed": 0, "throughput": 0.0}

        wall_time = time.time() - start_time

        # Parse results from simulation output
        metrics_file = output_dir / f"{scheduler}_batch_50_beta_1.0_cpu_1.0_duration_0.5_qps_{qps}" / "simulation_metrics.json"

        if metrics_file.exists():
            with open(metrics_file, 'r') as f:
                metrics = json.load(f)

            return {
                "scheduler": scheduler,
                "qps": qps,
                "tasks_completed": metrics.get("global_metrics", {}).get("total_tasks_completed", 0),
                "throughput": metrics.get("global_metrics", {}).get("avg_throughput_per_sec", 0.0),
                "avg_latency": metrics.get("latency_metrics", {}).get("scheduling_latency_mean", 0.0),
                "wall_time": wall_time,
                "azure_trace_used": True
            }
        else:
            print(f"    ⚠️  {scheduler} metrics file not found: {metrics_file}")
            return {"error": "no_metrics", "tasks_completed": 0, "throughput": 0.0}

    def _create_azure_config(self, scheduler: str, qps: float, duration_ms: int,
                            azure_trace_path: str, target_tasks: int = None,
                            cluster_config: str = "default_4_nodes") -> Path:
        """Create configuration file for Azure trace experiment."""

        # Different cluster configurations
        cluster_configs = {
            "default_4_nodes": {
                "num_nodes": 4,
                "node_types": [{"type": "test_node", "count": 4, "cores": 8, "memory": 32768, "disk": 50000, "slots": 4}]
            },
            "heterogeneous_100_nodes": {
                "num_nodes": 100,
                "node_types": [
                    {"type": "m510", "count": 40, "cores": 8, "memory": 65536, "disk": 100000, "slots": 8},
                    {"type": "xl170", "count": 25, "cores": 10, "memory": 65536, "disk": 100000, "slots": 10},
                    {"type": "c6525-25g", "count": 18, "cores": 16, "memory": 131072, "disk": 200000, "slots": 16},
                    {"type": "c6620", "count": 17, "cores": 28, "memory": 131072, "disk": 200000, "slots": 28}
                ]
            }
        }

        config = {
            "experiment": {
                "name": f"azure_validation_{scheduler}",
                "duration_ms": duration_ms,
                "warmup_duration_ms": 0,
                "seed": 12345,
                "timeout_ms": duration_ms,
                "target_completed_tasks": target_tasks,
                "replay_with_disk": False  # Match physical experiments --replay_with_disk=False
            },
            "scheduler": {
                "type": scheduler,
                "beta": 1.0,  # Match debug.sh
                "batch_size": 50,  # Match debug.sh
                "cpu_weight": 1.0,  # Match debug.sh
                "duration_weight": 0.5  # Match debug.sh
            },
            "cluster": cluster_configs[cluster_config],
            "workload": {
                "type": "trace",
                "trace_file": azure_trace_path,
                "target_qps": qps,
                "scaling_factor": 1.0
            },
            "output": {
                "log_level": "INFO",
                "metrics_file": "simulation_metrics.json"
            }
        }

        # Save config file
        config_file = self.output_base_dir / f"azure_config_{scheduler}_qps_{qps}.json"
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)

        return config_file

    def _validate_performance_rankings(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that scheduler performance rankings match expected order."""

        validation = {
            "expected_ranking": ["dodoor", "prequal", "sparrow", "power_of_two"],
            "actual_rankings": {},
            "ranking_correct": {},
            "performance_summary": {}
        }

        for qps_key, qps_results in results.items():
            if qps_key.startswith("qps_"):
                # Sort schedulers by throughput (descending)
                sorted_schedulers = sorted(
                    qps_results.items(),
                    key=lambda x: x[1].get("throughput", 0),
                    reverse=True
                )

                actual_ranking = [s[0] for s in sorted_schedulers]
                validation["actual_rankings"][qps_key] = actual_ranking

                # Check if ranking matches expectation
                expected = validation["expected_ranking"]
                matches_expected = self._rankings_match(actual_ranking, expected)
                validation["ranking_correct"][qps_key] = matches_expected

                # Performance summary
                validation["performance_summary"][qps_key] = {
                    scheduler: {
                        "throughput": results[qps_key][scheduler].get("throughput", 0),
                        "tasks_completed": results[qps_key][scheduler].get("tasks_completed", 0),
                        "rank": actual_ranking.index(scheduler) + 1 if scheduler in actual_ranking else None
                    }
                    for scheduler in expected
                }

        return validation

    def _rankings_match(self, actual: List[str], expected: List[str], tolerance: int = 1) -> bool:
        """Check if actual ranking matches expected with some tolerance."""

        # Allow for small ranking differences (tolerance positions)
        score = 0
        for i, scheduler in enumerate(expected):
            if scheduler in actual:
                actual_pos = actual.index(scheduler)
                expected_pos = i
                if abs(actual_pos - expected_pos) <= tolerance:
                    score += 1

        # Consider ranking correct if most schedulers are in expected positions
        return score >= len(expected) - 1

    def generate_validation_report(self, results: Dict[str, Any]) -> str:
        """Generate a comprehensive validation report."""

        report = []
        report.append("🔍 AZURE TRACE VALIDATION REPORT")
        report.append("=" * 50)

        config_name = results['experiment_metadata']['config_name']
        total_time = results['experiment_metadata']['total_time_seconds']
        azure_trace = results['experiment_metadata']['azure_trace_used']

        report.append(f"Experiment: {config_name}")
        report.append(f"Total time: {total_time:.1f}s")
        report.append(f"Azure trace: {azure_trace}")

        report.append("\n📊 PERFORMANCE SUMMARY:")
        ranking_validation = results['ranking_validation']

        for qps_key in ranking_validation['actual_rankings']:
            qps = qps_key.replace('qps_', '')
            report.append(f"\n  QPS {qps}:")

            expected = ranking_validation['expected_ranking']
            actual = ranking_validation['actual_rankings'][qps_key]
            correct = ranking_validation['ranking_correct'][qps_key]

            report.append(f"    Expected: {' > '.join(expected)}")
            report.append(f"    Actual:   {' > '.join(actual)}")
            report.append(f"    Correct:  {'✅' if correct else '❌'}")

            performance = ranking_validation['performance_summary'][qps_key]
            report.append(f"    Performance:")
            for scheduler in expected:
                if scheduler in performance:
                    perf = performance[scheduler]
                    report.append(f"      {scheduler}: {perf['tasks_completed']} tasks, "
                                 f"{perf['throughput']:.1f} tasks/sec (rank {perf['rank']})")

        report.append(f"\n🎯 VALIDATION RESULT:")
        all_correct = all(ranking_validation['ranking_correct'].values())
        report.append(f"   {'✅ ALL RANKINGS CORRECT' if all_correct else '❌ SOME RANKINGS INCORRECT'}")

        return "\n".join(report)


def main():
    """Run comprehensive Azure trace validation experiments."""

    print("🚀 Azure Trace Validation Framework")
    print("=" * 50)

    framework = AzureValidationFramework()

    # Quick validation first
    print("\n1️⃣ Running Quick Validation (5 minutes)...")
    quick_results = framework.run_validation_experiment("quick_validation")
    quick_report = framework.generate_validation_report(quick_results)
    print(quick_report)

    # Performance ranking validation
    print(f"\n2️⃣ Running Performance Ranking Validation (10 minutes)...")
    ranking_results = framework.run_validation_experiment("performance_ranking")
    ranking_report = framework.generate_validation_report(ranking_results)
    print(ranking_report)

    print(f"\n✅ Azure trace validation completed!")
    print(f"📊 All results saved in: simulator/debug/validation/")

    return quick_results, ranking_results


if __name__ == "__main__":
    quick_results, ranking_results = main()

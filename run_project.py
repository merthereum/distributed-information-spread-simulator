from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from src.simulator import DistSpreadSimulator, SimulationConfig


ROOT = Path(__file__).resolve().parent
RESULTS_DIR = ROOT / "results"


def load_project_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def run_experiments(
    repeats: int,
    nodes_count: int,
    timeout_ms: float,
    settings: dict,
    scenarios: List[Tuple[str, float, float]],
) -> tuple[pd.DataFrame, Dict[str, list]]:
    rows = []
    curves: Dict[str, list] = {}
    for scenario_idx, (scenario, plp, nfp) in enumerate(scenarios):
        for algorithm in DistSpreadSimulator.ALGORITHMS:
            for repeat in range(repeats):
                # The same seed is used for all algorithms in one scenario/repeat,
                # so the failed-node set is identical and comparisons are fair.
                seed = 10_000 + scenario_idx * 1_000 + repeat
                cfg = SimulationConfig(
                    nodes_count=nodes_count,
                    packet_loss_probability=plp,
                    node_failure_probability=nfp,
                    fanout=int(settings["fanout"]),
                    gossip_interval_ms=float(settings["gossip_interval_ms"]),
                    timeout_ms=timeout_ms,
                    min_delay_ms=float(settings["min_delay_ms"]),
                    max_delay_ms=float(settings["max_delay_ms"]),
                    serialization_ms=float(settings["serialization_ms"]),
                    multicast_group_size=int(settings["multicast_group_size"]),
                    seed=seed,
                )
                result = DistSpreadSimulator(cfg).run(algorithm)
                row = result.row()
                row["scenario"] = scenario
                row["repeat"] = repeat + 1
                rows.append(row)
                if repeat == 0:
                    curves[f"{scenario}|{algorithm}"] = result.curve
                print(
                    f"[{scenario}] {algorithm} run {repeat + 1}/{repeats}: "
                    f"coverage(active)={result.coverage_active_pct:.2f}% "
                    f"time={result.duration_ms:.2f} ms sent={result.messages_sent}"
                )
    return pd.DataFrame(rows), curves


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = [
        "active_nodes", "failed_nodes", "duration_ms", "coverage_active_pct",
        "coverage_total_pct", "messages_sent", "packets_lost",
        "failed_deliveries", "duplicates", "rounds", "p95_delivery_ms",
    ]
    return (
        df.groupby(
            ["scenario", "algorithm", "nodes_count", "packet_loss_probability", "node_failure_probability"],
            sort=False,
        )[numeric_cols]
        .mean()
        .reset_index()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run distributed information dissemination experiments")
    parser.add_argument("--config", type=Path, default=ROOT / "config.json")
    parser.add_argument("--repeats", type=int, default=None)
    parser.add_argument("--nodes", type=int, default=None)
    parser.add_argument("--timeout-ms", type=float, default=None)
    parser.add_argument("--quick", action="store_true", help="Run 3 repeats for a quick video demonstration")
    args = parser.parse_args()

    project_cfg = load_project_config(args.config)
    repeats = 3 if args.quick else (args.repeats or int(project_cfg["repeats"]))
    nodes_count = args.nodes or int(project_cfg["nodes_count"])
    timeout_ms = args.timeout_ms or float(project_cfg["timeout_ms"])
    scenarios = [
        (item["name"], float(item["packet_loss_probability"]), float(item["node_failure_probability"]))
        for item in project_cfg["scenarios"]
    ]
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    df, curves = run_experiments(repeats, nodes_count, timeout_ms, project_cfg, scenarios)
    summary = aggregate(df)
    raw_path = RESULTS_DIR / "raw_results.csv"
    summary_path = RESULTS_DIR / "summary_results.csv"
    curves_path = RESULTS_DIR / "curves.json"
    df.to_csv(raw_path, index=False)
    summary.to_csv(summary_path, index=False)
    curves_path.write_text(json.dumps(curves, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\nSaved:")
    print(raw_path)
    print(summary_path)
    print(curves_path)

    from plot_results import generate_all_plots
    generate_all_plots(summary_path, curves_path)


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
FIGURES_DIR = ROOT / "figures"

ALGORITHM_ORDER = [
    "Single Cast",
    "Hierarchical Multicast",
    "Broadcast Flooding",
    "Gossip Push",
    "Adaptive Gossip Push-Pull",
    "Hybrid Multicast-Gossip",
]
SHORT = {
    "Single Cast": "Single Cast",
    "Hierarchical Multicast": "Multicast",
    "Broadcast Flooding": "Broadcast",
    "Gossip Push": "Gossip Push",
    "Adaptive Gossip Push-Pull": "Adaptive Push-Pull",
    "Hybrid Multicast-Gossip": "Hybrid Mcast-Gossip",
}
SCENARIO_ORDER = [
    "S0: идеальная сеть",
    "S1: потеря пакетов 10%",
    "S2: потеря 20%, отказ 10%",
    "S3: потеря 30%, отказ 20%",
]


def _save(fig: plt.Figure, name: str) -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / name, dpi=240, bbox_inches="tight")
    plt.close(fig)


def _grouped_bars(df: pd.DataFrame, metric: str, ylabel: str, title: str, name: str, logy: bool = False) -> None:
    x = np.arange(len(SCENARIO_ORDER))
    width = 0.12
    fig, ax = plt.subplots(figsize=(12.2, 6.3))
    for i, algorithm in enumerate(ALGORITHM_ORDER):
        vals = []
        for scenario in SCENARIO_ORDER:
            row = df[(df.scenario == scenario) & (df.algorithm == algorithm)]
            vals.append(float(row.iloc[0][metric]))
        ax.bar(x + (i - 2.5) * width, vals, width, label=SHORT[algorithm])
    ax.set_xticks(x)
    ax.set_xticklabels([s.split(":", 1)[0] for s in SCENARIO_ORDER])
    ax.set_ylabel(ylabel)
    ax.set_xlabel("Сценарий эксперимента")
    ax.set_title(title)
    if logy:
        ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.28)
    ax.legend(ncol=2, fontsize=9)
    _save(fig, name)


def coverage_plot(df: pd.DataFrame) -> None:
    _grouped_bars(
        df, "coverage_total_pct", "Покрытие исходной конфигурации, %",
        "Итоговое покрытие сети по алгоритмам и сценариям (200 узлов, среднее по 10 запускам)",
        "fig1_coverage.png",
    )


def time_plot(df: pd.DataFrame) -> None:
    _grouped_bars(
        df, "duration_ms", "Время стабилизации, мс",
        "Время распространения информации (логарифмическая шкала)",
        "fig2_time.png", logy=True,
    )


def dynamics_plot(curves: Dict[str, List[List[float]]], scenario: str, name: str, title: str) -> None:
    fig, ax = plt.subplots(figsize=(11.8, 6.2))
    for algorithm in ALGORITHM_ORDER:
        key = f"{scenario}|{algorithm}"
        curve = curves[key]
        xs = [p[0] for p in curve]
        ys = [p[1] for p in curve]
        ax.step(xs, ys, where="post", linewidth=1.8, label=SHORT[algorithm])
    ax.set_xlabel("Время, мс")
    ax.set_ylabel("Покрытие доступных узлов, %")
    ax.set_ylim(0, 103)
    ax.grid(alpha=0.28)
    ax.set_title(title)
    ax.legend(ncol=2, fontsize=9)
    _save(fig, name)


def traffic_plot(df: pd.DataFrame) -> None:
    scenario = SCENARIO_ORDER[2]
    d = df[df.scenario == scenario].set_index("algorithm").loc[ALGORITHM_ORDER]
    x = np.arange(len(ALGORITHM_ORDER))
    delivered = d.messages_sent - d.packets_lost - d.failed_deliveries
    fig, ax = plt.subplots(figsize=(12, 6.2))
    ax.bar(x, delivered, label="Доставлено/обработано")
    ax.bar(x, d.packets_lost, bottom=delivered, label="Потеряно в канале")
    ax.bar(x, d.failed_deliveries, bottom=delivered + d.packets_lost, label="Направлено отказавшим узлам")
    ax.set_xticks(x)
    ax.set_xticklabels([SHORT[a] for a in ALGORITHM_ORDER], rotation=18, ha="right")
    ax.set_yscale("log")
    ax.set_ylabel("Количество сообщений (логарифмическая шкала)")
    ax.set_title("Структура сетевого трафика в комбинированном сценарии S2")
    ax.grid(axis="y", alpha=0.28)
    ax.legend()
    _save(fig, "fig5_traffic.png")


def duplicates_plot(df: pd.DataFrame) -> None:
    scenario = SCENARIO_ORDER[0]
    d = df[df.scenario == scenario].set_index("algorithm").loc[ALGORITHM_ORDER]
    x = np.arange(len(ALGORITHM_ORDER))
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    bars = ax.bar(x, d.duplicates)
    ax.set_xticks(x)
    ax.set_xticklabels([SHORT[a] for a in ALGORITHM_ORDER], rotation=18, ha="right")
    ax.set_ylabel("Количество дубликатов")
    ax.set_title("Дублированные сообщения в идеальной сети")
    ax.grid(axis="y", alpha=0.28)
    for bar, value in zip(bars, d.duplicates):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f"{value:.0f}", ha="center", va="bottom", fontsize=9)
    _save(fig, "fig6_duplicates.png")


def efficiency_plot(df: pd.DataFrame) -> None:
    d = df.copy()
    d["messages_per_reached"] = d.messages_sent / np.maximum(1.0, d.coverage_active_pct / 100.0 * d.active_nodes)
    fig, ax = plt.subplots(figsize=(11.8, 6.2))
    for algorithm in ALGORITHM_ORDER:
        sub = d[d.algorithm == algorithm].set_index("scenario").loc[SCENARIO_ORDER]
        ax.plot(range(len(SCENARIO_ORDER)), sub.messages_per_reached, marker="o", linewidth=1.8, label=SHORT[algorithm])
    ax.set_xticks(range(len(SCENARIO_ORDER)))
    ax.set_xticklabels([s.split(":", 1)[0] for s in SCENARIO_ORDER])
    ax.set_yscale("log")
    ax.set_ylabel("Сообщений на один достигнутый узел")
    ax.set_xlabel("Сценарий")
    ax.set_title("Коммуникационная эффективность алгоритмов")
    ax.grid(alpha=0.28)
    ax.legend(ncol=2, fontsize=9)
    _save(fig, "fig7_efficiency.png")


def generate_all_plots(summary_csv: Path | str, curves_json: Path | str) -> None:
    summary = pd.read_csv(summary_csv)
    curves = json.loads(Path(curves_json).read_text(encoding="utf-8"))
    coverage_plot(summary)
    time_plot(summary)
    dynamics_plot(
        curves, SCENARIO_ORDER[0], "fig3_dynamics_ideal.png",
        "Динамика распространения: идеальная сеть S0 (первый запуск)",
    )
    dynamics_plot(
        curves, SCENARIO_ORDER[2], "fig4_dynamics_faults.png",
        "Динамика распространения: потеря 20% и отказ 10% S2 (первый запуск)",
    )
    traffic_plot(summary)
    duplicates_plot(summary)
    efficiency_plot(summary)
    print(f"Figures saved to {FIGURES_DIR}")


if __name__ == "__main__":
    generate_all_plots(ROOT / "results" / "summary_results.csv", ROOT / "results" / "curves.json")

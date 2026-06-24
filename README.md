# DistSpreadLab_Mert_Er

Лабораторный проект по дисциплине «Распределённые системы и облачные вычислительные платформы».

Проект моделирует распространение одного информационного сообщения в сети из 200 логических узлов. Реализованы шесть алгоритмов:

1. Single Cast;
2. Hierarchical Multicast;
3. Broadcast Flooding;
4. Gossip Push;
5. Adaptive Gossip Push-Pull;
6. Hybrid Multicast-Gossip.

Для каждого запуска задаются вероятность потери пакета, вероятность отказа узла, сетевые задержки, fanout, интервал gossip-раундов и таймаут. Результаты сохраняются в CSV/JSON, после чего автоматически строятся графики.

## Быстрый запуск в VS Code

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python run_project.py --quick
```

Полный эксперимент, использованный в отчёте:

```powershell
python run_project.py --repeats 10
```

После выполнения появятся:

- `results/raw_results.csv` — все отдельные запуски;
- `results/summary_results.csv` — средние значения;
- `results/curves.json` — временные ряды покрытия;
- `figures/*.png` — графики для отчёта.

## Запуск в Google Colab

Откройте `DistSpreadLab_Colab.ipynb` в Google Colab и последовательно выполните все ячейки. В первой ячейке требуется загрузить ZIP-архив проекта.

## Структура

```text
DistSpreadLab/
├── src/simulator.py
├── run_project.py
├── plot_results.py
├── config.json
├── requirements.txt
├── DistSpreadLab_Colab.ipynb
├── results/
└── figures/
```

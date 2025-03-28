import functools
import itertools
from ntpath import exists
from pathlib import Path
import os
import logging
import re
from sys import implementation
import polars as pl
from typing import Dict, Generator, Set, Union
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

omen_dict_dir = Path("./benchmarks-omen-dict/")
omen_maps_dir = Path("./benchmarks-omen-maps/")
firefly1_dir = Path("./benchmarks-firefly1/")


implementations = ["central", "shard", "worker"]
scenarios = ["read-only", "read-write", "mixed-load"]
BENCHMARK_NAME = "ree"


# Set log level.
logging.basicConfig(level=logging.INFO)


def parse_file(filename: str) -> tuple[list[float], list[str], float]:
    """Parses a benchmark result file and returns a tuple with a list of results,
    and a list of parameters."""
    with open(filename) as f:
        lines = f.readlines()

    # First line is always "Parameters:".
    if lines[0] != "Parameters:\n":
        raise Exception(
            f"Expected first line 'Parameters:' in {filename}, got {lines[0]}"
        )

    # Print parameters, which are followed by an empty line.
    parameters = []
    parameters_end_at_line = 0  # line on which the parameters end
    for i in range(1, len(lines)):
        if lines[i] == "\n":
            parameters_end_at_line = i
            break
        parameters.append(lines[i])

        # Parse init time of server
    line_init = r"Server initialization wall clock time = ([\d\.]+) ms\n"
    mInit = re.match(line_init, lines[parameters_end_at_line + 1])
    if not mInit:
        raise Exception(
            f"Unexpected server initialization line ({filename}, line: {parameters_end_at_line + 1})"
        )
    server_time = float(mInit.group(1))

    # Parse results.
    # Each result consists of three lines:
    #     Starting benchmark $NAME: $INDEX
    #     Wall clock time = $TIME ms
    #     $NAME done
    line0 = r"Starting benchmark (\w+): (\d+)\n"
    line1 = r"Wall clock time = ([\d\.]+) ms\n"
    line2 = r"(\w+) done\n"
    results = []
    for i in range(parameters_end_at_line + 2, len(lines), 3):
        # Parse lines
        m0 = re.match(line0, lines[i])
        m1 = re.match(line1, lines[i + 1])
        m2 = re.match(line2, lines[i + 2])
        # Check if lines are as expected
        if not m0 or not m1 or not m2:
            raise Exception(
                f"Unexpected lines ({filename}, line {i}):\n"
                + "".join(lines[i : i + 3])
            )
        index = int(m0.group(2))
        if (index - 1) != (i - parameters_end_at_line - 1) // 3:
            raise Exception(
                f"Unexpected benchmark index ({filename}, line {i}): "
                + f"{index} (expected {i // 3})"
            )
        time = float(m1.group(1))
        results.append(time)

    return (results, parameters, server_time)


def plot_boxplots(
    speedups: dict[int, list[Union[int, float]]], title: str, xlabel: str, ylabel: str
):
    """Plot the speedups as boxplots.
    Example:
    Title = Speedup of readonly
    xlabel = number of threads
    ylabel = Speedup
    """
    mpl.rcParams.update({"font.size": 16})
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.boxplot([speedups[i] for i in speedups], notch=True, bootstrap=1000)
    # Note: the 'notch' represents the confidence interval, calculated using
    # bootstrapping, a statistical technique that resamples the data.
    # Note: matplotlib draws the whiskers at 1.5 * IQR from the 1st and 3rd quartile.
    # Points outside the whiskers are plotted as outliers (individual circles).
    # Different styles exist.
    # See https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.boxplot.html
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_xticks(list(speedups.keys()))
    ax.set_xticklabels(
        [str(k) if k % 4 == 0 or k == 1 else "" for k in speedups.keys()]
    )
    ax.set_ylim(0, 1.1 * max([max(speedups[i]) for i in speedups]))
    path = Path("report/images/boxplots")
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        f"{path.as_posix()}/boxplot-{
            title.replace(' ', '-').replace(':', '').replace(',', '').lower()
        }.pdf"
    )


def plot_speedup_violinplots(speedups: dict[int, list[float]]):
    """Plot the speedups as violin plots (alternative)."""
    mpl.rcParams.update({"font.size": 16})
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.violinplot([speedups[i] for i in speedups], showmedians=True)
    # See https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.violinplot.html
    ax.set_title(f"Speedup of {BENCHMARK_NAME}")
    ax.set_xlabel("Number of threads")
    ax.set_ylabel("Speedup")
    ax.set_xticks(list(speedups.keys()))
    ax.set_xticklabels(
        [str(k) if k % 4 == 0 or k == 1 else "" for k in speedups.keys()]
    )
    ax.set_ylim(0, 1.1 * max([max(speedups[i]) for i in speedups]))
    fig.savefig(f"speedup-{BENCHMARK_NAME}-violinplot.pdf")


def plot_speedup_errorbars(speedups: dict[int, list[float]]):
    """Plot the speedups as plot with error bars."""
    mpl.rcParams.update({"font.size": 16})
    fig, ax = plt.subplots(figsize=(10, 5))
    x = list(speedups.keys())
    medians = [np.median(speedups[i]) for i in speedups]
    errors_down = [
        np.median(speedups[i]) - np.quantile(speedups[i], 0.25) for i in speedups
    ]
    errors_up = [
        np.quantile(speedups[i], 0.75) - np.median(speedups[i]) for i in speedups
    ]
    ax.errorbar(x, y=medians, yerr=[errors_down, errors_up])
    # See https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.violinplot.html
    ax.set_title(f"Speedup of {BENCHMARK_NAME}")
    ax.set_xlabel("Number of threads")
    ax.set_ylabel("Speedup")
    ax.set_xticks(x)
    ax.set_xticklabels([str(k) if k % 4 == 0 or k == 1 else "" for k in x])
    ax.set_ylim(0, 1.1 * max([max(speedups[i]) for i in speedups]))
    fig.savefig(f"speedup-{BENCHMARK_NAME}-errorbars.pdf")


def parse_files(fileNames: Generator[Path, None, None], path: Path):
    read_write_regex = r"-rw(\d+)\.txt"
    read_regex = r"-r(\d+)\.txt"
    mix_regex = r"-mix(\d+)\.txt"
    params: Dict[str, Dict[str, Set[str]]] = {
        "read-only": {},
        "read-write": {},
        "mixed-load": {},
    }
    results = {
        "read-only": {},
        "read-write": {},
        "mixed-load": {},
    }

    server_time = {
        "read-only": {},
        "read-write": {},
        "mixed-load": {},
    }
    for file in fileNames:
        rw = re.search(read_write_regex, file.name)
        r = re.search(read_regex, file.name)
        mix = re.search(mix_regex, file.name)
        if not rw and not r and not mix:
            raise Exception("Unknown type of test")
        if rw:
            key = "read-write"
            i = int(rw.group(1))
        elif r:
            key = "read-only"
            i = int(r.group(1))
        elif mix:
            key = "mixed-load"
            i = int(mix.group(1))
        subkey = file.name.split("-")[0]
        if subkey not in results[key]:
            results[key][subkey] = {}
            server_time[key][subkey] = {}
            params[key][subkey] = set()
        results[key][subkey][i], tmp_params, server_time[key][subkey][i] = parse_file(
            os.path.join(path, file.name)
        )
        params[key][subkey] = params[key][subkey] | set(tmp_params)
        if len(params[key][subkey]) > 3:
            raise Exception(f"Inconsistent params for test: {key} index: {i}")
    return (results, server_time)


def parse_dirs(dirs: list[Path]):
    measurements = {}
    server = {}
    for dir in dirs:
        measurements[dir.name], server[dir.name] = parse_files(dir.iterdir(), dir)
    return (measurements, server)


Measurements = Dict[str, Dict[str, Dict[str, Dict[int, list[float]]]]]
Server = Dict[str, Dict[str, Dict[str, Dict[int, float]]]]


def measurements_to_lf(measurements: Measurements):
    frames = []
    for directory, scenarios in measurements.items():
        dirsplit = directory.split("-")
        device = dirsplit[1]
        storage = dirsplit[2]
        for scenario, implementations in scenarios.items():
            for implementation, benchmarks in implementations.items():
                for threads, _measurements in benchmarks.items():
                    for index, measurement in enumerate(_measurements):
                        row = {
                            "device": device,
                            "storage": storage,
                            "scenario": scenario,
                            "implementation": implementation,
                            "scheduler_threads": threads,
                            "experiment": index + 1,
                            "elapsed_time(ms)": measurement,
                        }
                        frames.append(row)
    return pl.DataFrame(frames).lazy()


def server_measurements_to_lf(server: Server):
    frames = []
    for directory, scenarios in server.items():
        dirsplit = directory.split("-")
        device = dirsplit[1]
        storage = dirsplit[2]
        for scenario, implementations in scenarios.items():
            for implementation, benchmarks in implementations.items():
                for threads, measurement in benchmarks.items():
                    row = {
                        "device": device,
                        "storage": storage,
                        "scenario": scenario,
                        "implementation": implementation,
                        "scheduler_threads": threads,
                        "elapsed_time(ms)": measurement,
                    }
                    frames.append(row)
    return pl.DataFrame(frames).lazy()


def read_csv(filename: str):
    return pl.scan_csv(filename)


def compare_storage(lf_origin: pl.LazyFrame):
    # conclusion:
    # maps tend to be better under most circumstances. However on 22 instances
    # of the 576 dict was better. All but one were with the worker
    # implementation. On the 22 instances the maps implementation ranged from
    # 0.04 to 21% slower with only two instances being more than 4%.
    # On average maps was 10% faster across all implementations, scenarios and
    # threads
    """
    lf = (
        lf_origin.filter(pl.col("device") == "omen")
        .group_by(
            [pl.col("storage", "implementation", "scenario", "scheduler_threads")]
        )
        .agg(pl.col("elapsed_time(ms)").mean())
    ).cache()

    """

    lf_independant = (
        lf_origin.filter(pl.col("device") == "omen")
        .group_by(pl.col("storage"))
        .agg(pl.col("elapsed_time(ms)").mean())
    )

    lf_in_dict = lf_independant.filter(pl.col("storage") == "dict").select(
        pl.col("elapsed_time(ms)").alias("dict_time")
    )
    lf_in_maps = lf_independant.filter(pl.col("storage") == "maps").select(
        pl.col("elapsed_time(ms)").alias("maps_time")
    )

    lf_in_compare = pl.concat([lf_in_maps, lf_in_dict], how="horizontal").with_columns(
        ((pl.col("dict_time") - pl.col("maps_time")) / pl.col("dict_time") * 100).alias(
            "diff(%)"
        )
    )
    print(lf_in_compare.collect())

    """
    lf_dict = lf.filter(pl.col("storage") == "dict").select(
        [
            "implementation",
            "scenario",
            "scheduler_threads",
            pl.col("elapsed_time(ms)").alias("dict_time"),
        ]
    )
    lf_maps = lf.filter(pl.col("storage") == "maps").select(
        [
            "implementation",
            "scenario",
            "scheduler_threads",
            pl.col("elapsed_time(ms)").alias("maps_time"),
        ]
    )

    compare = (
        lf_maps.join(
            lf_dict, on=["implementation", "scenario", "scheduler_threads"], how="inner"
        )
        .with_columns(
            (
                (pl.col("dict_time") - pl.col("maps_time")) / pl.col("dict_time") * 100
            ).alias("diff(%)")
        )
        .sort(pl.last(), descending=True)
    )

    """

    # print(compare.collect())
    # print(compare.collect().count())

    """

    negatives = compare.filter(pl.col("diff(%)") < 0).sort(
        [pl.col("diff(%)")], descending=True
    )
    print(negatives.collect().to_pandas().to_string())
    """
    return


def calc_speedup(orig: pl.LazyFrame):
    base = (
        orig.filter(pl.col("scheduler_threads").eq(1))
        .select("elapsed_time(ms)")
        .median()
        .collect()
        .get_column("elapsed_time(ms)")[0]
    )
    return orig.with_columns((base / pl.col("elapsed_time(ms)")).alias("speedup"))


def calc_throughput(orig: pl.LazyFrame):
    pass


def process_results():
    data = read_csv("measurements.csv")
    data = (
        data.filter(pl.col("storage") == "maps")
        .with_columns(
            pl.col("scenario")
            .str.replace("rw", "gd")
            .str.replace("r", "read-only")
            .str.replace("gd", "read-write")
            .str.replace("mix", "mixed-load")
            .alias("scenario")
        )
        .cache()
    )
    combinations = list(itertools.product(implementations, scenarios))
    lfs = [
        data.filter(
            (
                (pl.col("implementation") == implementation)
                & (pl.col("scenario") == scenario)
            )
        )
        for (implementation, scenario) in combinations
    ]
    speedups = [calc_speedup(lf).sort(pl.col("scheduler_threads")) for lf in lfs]
    for s in speedups:
        tmp = s.select(["implementation", "scenario"]).unique().collect()
        implementation = tmp.get_column("implementation")[0]
        scenario = tmp.get_column("scenario")[0]
        lf = s.group_by(pl.col("scheduler_threads")).agg(pl.col("speedup")).collect()
        threads_col = lf.get_column("scheduler_threads")
        speedups_col = lf.get_column("speedup")
        plot_data = {threads_col[i]: speedups_col[i] for i in range(len(threads_col))}
        plot_boxplots(
            plot_data,
            title=f"Speedup of scenario: {scenario}, implementation: {implementation}",
            xlabel="Number of threads",
            ylabel="Speedup",
        )


def main():
    process_results()


if __name__ == "__main__":
    main()

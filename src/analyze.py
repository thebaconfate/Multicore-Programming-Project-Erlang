import itertools
from pathlib import Path
import os
import logging
import re
from numpy.typing import NDArray
import polars as pl
from typing import Dict, Generator, List, Set, Tuple, Union
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from polars import LazyFrame, lazyframe

omen_dict_dir = Path("./benchmarks-omen-dict/")
omen_maps_dir = Path("./benchmarks-omen-maps/")
firefly1_dir = Path("./benchmarks-firefly1/")
firefly2_dir = Path("./benchmarks-firefly-session-2")

data_file = Path("measurements.csv")


implementations = ["central", "sharded", "worker"]
scenarios = ["read-only", "read-write", "mixed-load"]


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
    parameters_end_at_line += 2
    server_time = float(mInit.group(1))
    for i in range(parameters_end_at_line, len(lines)):
        if lines[i] == "\n":
            parameters_end_at_line = i
            break
        parameters.append(lines[i])

    # Parse results.
    # Each result consists of three lines:
    #     Starting benchmark $NAME: $INDEX
    #     Wall clock time = $TIME ms
    #     $NAME done
    line0 = r"Starting benchmark (\w+): (\d+)\n"
    line1 = r"Wall clock time = ([\d\.]+) ms\n"
    line2 = r"(\w+) done\n"
    results = []
    for i in range(parameters_end_at_line + 1, len(lines), 3):
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


def title_to_filename(title: str):
    return title.replace(" ", "-").replace(":", "").replace(",", "").lower()


def plot_boxplots(
    speedups: dict[str, list[Union[int, float]]],
    title: str,
    xlabel: str,
    ylabel: str,
    dir: str = "",
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
    path = Path(f"report/images/boxplots/{dir}")
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    fig.savefig(f"{path.as_posix()}/boxplot-{title_to_filename(title)}.pdf")
    plt.close()


def plot_violinplots(
    speedups: dict[str, list[float]],
    title: str,
    xlabel: str,
    ylabel: str,
    dir: str = "",
):
    """Plot the speedups as violin plots (alternative).
    Example:
    Title = Speedup of readonly
    xlabel = number of threads
    ylabel = Speedup
    """

    mpl.rcParams.update({"font.size": 16})
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.violinplot([speedups[i] for i in speedups], showmedians=True)
    # See https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.violinplot.html
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_xticks(list(speedups.keys()))
    ax.set_xticklabels(
        [str(k) if k % 4 == 0 or k == 1 else "" for k in speedups.keys()]
    )
    ax.set_ylim(0, 1.1 * max([max(speedups[i]) for i in speedups]))
    path = Path(f"report/images/violinplots/{dir}")
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    fig.savefig(f"{path.as_posix()}/{title_to_filename(title)}-violinplot.pdf")
    plt.close()


def plot_errorbars(
    speedups: dict[str, list[float]],
    title: str,
    xlabel: str,
    ylabel: str,
    dir: str = "",
):
    """Plot the speedups as plot with error bars.
    Example:
    Title = Speedup of readonly
    xlabel = number of threads
    ylabel = Speedup
    """
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
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels([str(k) if k % 4 == 0 or k == 1 else "" for k in x])
    ax.set_ylim(0, 1.1 * max([max(speedups[i]) for i in speedups]))
    path = Path(f"report/images/errorbars/{dir}")
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    fig.savefig(f"{path.as_posix()}/{title_to_filename(title)}-errorbars.pdf")
    plt.close()


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
        # Delete this
        params[key][subkey] = params[key][subkey] | set(tmp_params)
        if len(params[key][subkey]) > 6:
            print(params[key][subkey])
            raise Exception(f"Inconsistent params for test: {key} index: {i}")
    return (results, params, server_time)


Measurements = Dict[str, Dict[str, Dict[str, Dict[int, list[float]]]]]
Server = Dict[str, Dict[str, Dict[str, Dict[int, float]]]]
Params = Dict[str, Dict[str, Dict[str, Set[str]]]]


def parse_dirs(dirs: list[Path]) -> Tuple[Measurements, Params, Server]:
    measurements = {}
    server = {}
    params = {}
    for dir in dirs:
        if not dir.exists():
            dir = Path(f"src/{dir.name}")
        measurements[dir.name], params[dir.name], server[dir.name] = parse_files(
            dir.iterdir(), dir
        )
    return (measurements, params, server)


def measurements_to_lf(measurements: Measurements, params: Params):
    frames = []
    for directory, scenarios in measurements.items():
        dirsplit = directory.split("-")
        device = dirsplit[1]
        storage = dirsplit[2]
        for scenario, implementations in scenarios.items():
            for implementation, benchmarks in implementations.items():
                for threads, _measurements in benchmarks.items():
                    for index, measurement in enumerate(_measurements):
                        list_of_params = [
                            i.split(": ")
                            for i in params[directory][scenario][implementation]
                        ]
                        dict_of_params = {
                            " ".join(i[0].split(" ")[2:]).lower(): int(i[1])
                            for i in list_of_params
                        }
                        row = {
                            "device": device,
                            "storage": storage,
                            "scenario": scenario,
                            "implementation": implementation,
                            "scheduler_threads": threads,
                            "experiment": index + 1,
                            "elapsed_time(ms)": measurement,
                        } | dict_of_params
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
    lfs = [
        orig.filter(
            (pl.col("implementation") == implementation)
            & (pl.col("scenario") == scenario)
        )
        for (implementation, scenario) in combinations()
    ]
    return pl.concat(
        [
            lf.with_columns(
                (
                    (
                        lf.filter(pl.col("scheduler_threads").eq(1))
                        .select("elapsed_time(ms)")
                        .median()
                        .collect()
                        .get_column("elapsed_time(ms)")[0]
                    )
                    / pl.col("elapsed_time(ms)")
                ).alias("speedup")
            )
            for lf in lfs
        ]
    )


def combinations():
    return itertools.product(implementations, scenarios)


def lf_combinations(lf):
    return [
        lf.filter(
            (
                (pl.col("implementation") == implementation)
                & (pl.col("scenario") == scenario)
            )
        )
        for (implementation, scenario) in combinations()
    ]


def make_speedup_plots(origin: pl.LazyFrame = pl.scan_csv(data_file.name)):
    data = origin.filter(pl.col("storage") == "maps").cache()
    lfs = [lf.sort(["scheduler_threads"]) for lf in lf_combinations(data)]
    device = origin.select("device").unique().collect().get_column("device")[0]
    for s in lfs:
        tmp = s.select(["implementation", "scenario"]).unique().collect()
        implementation = tmp.get_column("implementation")[0]
        scenario = tmp.get_column("scenario")[0]
        lf = s.group_by(pl.col("scheduler_threads")).agg(pl.col("speedup")).collect()
        threads_col = lf.get_column("scheduler_threads")
        speedups_col = lf.get_column("speedup")
        plot_data = {threads_col[i]: speedups_col[i] for i in range(len(threads_col))}
        title = f"Speedup of scenario: {scenario}, implementation: {implementation}"
        xlabel = "Number of threads"
        ylabel = "Speedup"
        plot_boxplots(plot_data, title=title, xlabel=xlabel, ylabel=ylabel, dir=device)
        plot_violinplots(
            plot_data, title=title, xlabel=xlabel, ylabel=ylabel, dir=device
        )
        plot_errorbars(plot_data, title=title, xlabel=xlabel, ylabel=ylabel, dir=device)


def calc_throughput(orig: pl.LazyFrame) -> pl.LazyFrame:
    return orig.with_columns(
        (
            (pl.col("operations per client") * pl.col("clients"))
            / (pl.col("elapsed_time(ms)") / 1000)
        ).alias("throughput(s)")
    )


def make_throughput_plots(
    orig_lf: pl.LazyFrame = calc_throughput(pl.scan_csv(data_file.name)),
):
    orig_lf.cache()
    lfs = [lf.sort(pl.col("scheduler_threads")) for lf in lf_combinations(orig_lf)]
    device = orig_lf.select("device").unique().collect().get_column("device")[0]
    for lf in lfs:
        tmp = lf.select(["implementation", "scenario"]).unique().collect()
        implementation = tmp.get_column("implementation")[0]
        scenario = tmp.get_column("scenario")[0]
        df = (
            lf.group_by(pl.col("scheduler_threads"))
            .agg(pl.col("throughput(s)"))
            .collect()
        )
        threads_col = df.get_column("scheduler_threads")
        throughput_col = df.get_column("throughput(s)")
        plot_data = {threads_col[i]: throughput_col[i] for i in range(len(threads_col))}
        title = f"Throughput per second of scenario: {scenario}, implementation: {implementation}"
        xlabel = "Number of threads"
        ylabel = "Throughput (ops/s)"
        plot_boxplots(plot_data, title=title, xlabel=xlabel, ylabel=ylabel, dir=device)
        plot_violinplots(
            plot_data, title=title, xlabel=xlabel, ylabel=ylabel, dir=device
        )
        plot_errorbars(plot_data, title=title, xlabel=xlabel, ylabel=ylabel, dir=device)


def compare_throughput(orig_lf: pl.LazyFrame, scheduler_threads: int):
    device = orig_lf.select("device").unique().collect().get_column("device")[0]
    orig_lf = (
        orig_lf.filter(pl.col("scheduler_threads") == scheduler_threads)
        .group_by(["implementation", "scenario"])
        .agg(pl.col("throughput(s)"))
    )
    lfs = lf_combinations(orig_lf)
    plot_data: Dict[str, List[float]] = {}
    for lf in lfs:
        df = lf.collect()
        implementation = df.get_column("implementation")[0]
        scenario = df.get_column("scenario")[0]
        throughput = df.get_column("throughput(s)")
        key = str(implementation) + ": " + str(scenario)
        plot_data[key] = throughput.to_list()[0]
    title = f"Throughput in operations per second for {scheduler_threads} " + "threads"
    xlabel = "implementation: scenario"
    ylabel = "Million operations per second"
    mpl.rcParams.update({"font.size": 16})
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.boxplot(list(plot_data.values()), notch=True, bootstrap=1000)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_xticks(range(0, len(plot_data.keys()) + 1))
    ax.set_xticklabels([""] + list(plot_data.keys()), rotation=80)
    ax.set_ylim(0, 1.1 * max([max(v) for v in plot_data.values()]))
    path = Path(f"report/images/boxplots/{device}")
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    plt.subplots_adjust(bottom=0.4)
    fig.savefig(f"{path.as_posix()}/boxplot-{title_to_filename(title)}.pdf")
    plt.close()


def renew_omen_maps_results():
    msms, params, server = parse_dirs([omen_maps_dir])
    lf = pl.scan_csv(data_file.name).filter(pl.col("storage") != "maps")
    new_lf = measurements_to_lf(msms, params)
    lf = pl.concat([lf, new_lf.select(lf.collect_schema().names())]).collect().lazy()
    lf.sink_csv(data_file.name)


def compute_speedup_diff(lf: pl.LazyFrame):
    lf = lf.group_by(
        "device", "implementation", "scenario", "scheduler_threads", maintain_order=True
    ).agg(
        (
            pl.when(pl.col("scheduler_threads").eq(1))
            .then(pl.lit(1))
            .otherwise(pl.col("speedup"))
        )
        .median()
        .alias("median_speedup")
    )
    lfs = [
        lf.filter(
            (pl.col("implementation") == implementation)
            & (pl.col("scenario") == scenario)
        )
        for (implementation, scenario) in combinations()
    ]
    return pl.concat(
        [
            lf.with_columns(
                pl.col("median_speedup").shift(1).alias("prev_median_speedup")
            )
            .with_columns(
                (pl.col("median_speedup") - pl.col("prev_median_speedup")).alias(
                    "speedup_diff"
                )
            )
            .drop(["median_speedup", "prev_median_speedup"])
            .with_columns(
                pl.when(pl.col("speedup_diff").is_null())
                .then(pl.lit(0))
                .otherwise(pl.col("speedup_diff"))
                .alias("speedup_diff")
            )
            for lf in lfs
        ]
    )


def make_speedup_diff_plot(
    threads: List[int], data: List[float], dev: str, impl: str, scen: str
):
    title = f"Speedup difference implementation: {impl}, scenario: {scen}"
    xlabel = "Scheduler threads"
    ylabel = "Δ Speedup"
    mpl.rcParams.update({"font.size": 16})
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(threads, data, "o-", linewidth=2)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_xticks(range(0, len(threads) + 1))
    ax.set_xticklabels(["0"] + [str(t) for t in threads])
    ax.axhline(0, color="red", linestyle="--", linewidth=1)
    ax.set_ylim(1.1 * min(data), 1.1 * max(data))
    path = Path(f"report/images/plot/{dev}")
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    fig.savefig(f"{path.as_posix()}/plot-{title_to_filename(title)}.pdf")
    plt.close()


def make_speedup_diff_plots(orig: pl.LazyFrame):
    lfs = lf_combinations(orig)
    for lf in lfs:
        df = lf.collect()
        diffs = df.get_column("speedup_diff")
        threads = df.get_column("scheduler_threads")
        implementation = df.get_column("implementation")[0]
        scenario = df.get_column("scenario")[0]
        device = df.get_column("device")[0]
        make_speedup_diff_plot(
            threads.to_list(), diffs.to_list(), device, implementation, scenario
        )


def backup_measurements(filename: str):
    pl.scan_csv(filename).sink_csv(f"backup_{filename}")


def main():
    msms, params, server = parse_dirs([firefly2_dir])
    current_lf = pl.scan_csv(f"src/backup_{data_file.name}")
    new_lf = measurements_to_lf(msms, params)
    new_lf = new_lf.with_columns(
        (
            pl.when(pl.col("storage") == "session")
            .then(pl.lit("maps"))
            .otherwise(pl.col("storage"))
        ).alias("storage")
    ).select(current_lf.collect_schema().names())
    merged_lf = pl.concat([current_lf, new_lf])
    merged_lf.sink_csv(f"src/{data_file.name}")
    print(merged_lf.collect())


if __name__ == "__main__":
    main()

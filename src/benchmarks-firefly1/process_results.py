#!/usr/bin/env python3
# This is a Python script that parses the benchmark results, calculates some statistics,
# and generates graphs. You can use it as a template and extend it as necessary.
# (You are not required to use this.)

# To get started, you need to create a Python virtual environment and install the
# dependencies, like this:
# $ python3 -m venv venv
# $ source venv/bin/activate
# $ pip install -r requirements.txt
#
# We use numpy [1] for statistics and matplotlib [2] for plotting.
#
# When running this script, make sure the virtual environment is activated!
# $ source venv/bin/activate
# $ python3 process_results.py
#
# [1] https://numpy.org/
# [2] https://matplotlib.org/

import logging
import re
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt


# Name of the benchmark. File names should be be result-$NAME-$NUMBER.txt.
BENCHMARK_NAME = "mixedload"
# Number of benchmark files, e.g. result-fib-1.txt to result-fib-64.txt.
NUMBER_OF_FILES = 64
# Number of iterations per benchmark, i.e. number of results in one file.
NUMBER_OF_ITERATIONS = 30


# Set log level.
logging.basicConfig(level=logging.INFO)


def parse_file(filename: str, name: str) -> tuple[list[float], list[str]]:
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
        name0 = m0.group(1)
        if name0 != name:
            raise Exception(
                f"Unexpected benchmark name ({filename}, line {i}): {name0}"
            )
        name2 = m2.group(1)
        if name2 != name:
            raise Exception(
                f"Unexpected benchmark name ({filename}, line {i}): {name2}"
            )
        index = int(m0.group(2))
        if (index - 1) != (i - parameters_end_at_line - 1) // 3:
            raise Exception(
                f"Unexpected benchmark index ({filename}, line {i}): "
                + f"{index} (expected {i // 3})"
            )
        time = float(m1.group(1))
        results.append(time)

    if len(results) != NUMBER_OF_ITERATIONS:
        logging.warning(
            f"{filename} contains {len(results)} results,"
            + f"expected {NUMBER_OF_ITERATIONS}."
        )

    return (results, parameters)


def parse_files(name: str) -> dict[int, list[float]]:
    """Parse the benchmark files for the benchmark with the given name, and
    return the results."""
    results = {}
    parameters = {}
    for i in range(1, NUMBER_OF_FILES + 1):
        filename = f"result-{name}-{i}.txt"
        results[i], parameters[i] = parse_file(filename, name)
    # Check if parameters match across all files.
    for i in range(2, NUMBER_OF_FILES + 1):
        if parameters[i] != parameters[1]:
            raise Exception(
                f"Parameters in file {i} do not match parameters in file 1:\n"
                + f"{parameters[i]} != {parameters[1]}"
            )
    logging.info(f"Parameters:\n%s", "".join(parameters[1]))
    return results


def calculate_speedups(results: dict[int, list[float]]) -> dict[int, list[float]]:
    """Calculate the speedups for all results.

    The base time is the median of the results for 1 thread."""
    base = np.median(results[1])
    speedups = {i: [base / t for t in result] for (i, result) in results.items()}
    return speedups


def plot_speedup_boxplots(speedups: dict[int, list[float]]):
    """Plot the speedups as boxplots."""
    mpl.rcParams.update({"font.size": 16})
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.boxplot([speedups[i] for i in speedups], notch=True, bootstrap=1000)
    # Note: the 'notch' represents the confidence interval, calculated using
    # bootstrapping, a statistical technique that resamples the data.
    # Note: matplotlib draws the whiskers at 1.5 * IQR from the 1st and 3rd quartile.
    # Points outside the whiskers are plotted as outliers (individual circles).
    # Different styles exist.
    # See https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.boxplot.html
    ax.set_title(f"Speedup of {BENCHMARK_NAME}")
    ax.set_xlabel("Number of threads")
    ax.set_ylabel("Speedup")
    ax.set_xticks(list(speedups.keys()))
    ax.set_xticklabels([k if k % 4 == 0 or k == 1 else "" for k in speedups.keys()])
    ax.set_ylim(0, 1.1 * max([max(speedups[i]) for i in speedups]))
    fig.savefig(f"speedup-{BENCHMARK_NAME}-boxplot.pdf")


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
    ax.set_xticklabels([k if k % 4 == 0 or k == 1 else "" for k in speedups.keys()])
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
    ax.set_xticklabels([k if k % 4 == 0 or k == 1 else "" for k in x])
    ax.set_ylim(0, 1.1 * max([max(speedups[i]) for i in speedups]))
    fig.savefig(f"speedup-{BENCHMARK_NAME}-errorbars.pdf")


def main():
    # Parse benchmark results.
    results = parse_files(BENCHMARK_NAME)
    logging.info("Results: %s", results)

    # Calculate results.
    speedups = calculate_speedups(results)
    logging.info("Speedups: %s", speedups)

    # Calculate and print 1st quartile, median, and 3rd quartile.
    # These may be useful for the report.
    speedup_quartiles = {
        i: np.quantile(speedups[i], [0.25, 0.5, 0.75]) for i in speedups
    }
    logging.info("Speedup quartiles: %s", speedup_quartiles)

    # Plot results using box plots.
    plot_speedup_boxplots(speedups)

    # Alternative: plot using violin plots.
    plot_speedup_violinplots(speedups)

    # Alternative: plot using graph with error bars.
    plot_speedup_errorbars(speedups)


if __name__ == "__main__":
    main()

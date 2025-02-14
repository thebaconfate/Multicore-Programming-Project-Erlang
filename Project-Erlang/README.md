# Key-Value store

*Multicore Programming 2025 - Erlang Project*

This directory contains an implementation of the key-value store with one server process.

It contains the following files:
* server.erl: the protocol that is used to interact with an implementation of the service.
* server_centralized.erl: an implementation of the service that only uses one Erlang process.
* benchmark.erl: a starting point for benchmarks of your project.
* run_benchmarks.sh: a script that runs the benchmarks and stores the results to files.
* run.sh and run.bat: helper scripts run your files, like seen in class.
* Makefile: a Makefile that compiles the project:
    - make all: compile the project.
    - make test: run the accompanying tests.
    - make benchmark: compile the project and run the benchmarks.
    - make clean: remove all compiled files.

To visualize benchmark results, we also provide a Python script that uses [matplotlib](https://matplotlib.org/):
* benchmarks/process_results.py: a Python script that parses benchmark results, calculates
  statistics, and generates plots.
* benchmarks/requirements.txt: a file listing the dependencies of the Python script, for use with pip.

To run the Python script, you first need to install the dependencies, preferably in a virtual environment:

```sh
$ cd benchmarks
# Create a new virtual environment in the folder venv:
$ python3 -m venv venv
# Activate the virtual environment in the current shell session:
$ source venv/bin/activate
# Install the dependencies:
$ python -m pip install -r requirements.txt
```

Every time you create a new terminal session, you'll need to activate the virtual environment first:

```sh
$ source venv/bin/activate
# You can now run:
$ python process_results.py
```

The script expects the benchmark results to be in files with names like `result-fib-1.txt`, in the benchmarks folder. The `run_benchmarks.sh` script puts them there.

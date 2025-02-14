-module(benchmark).

-export([test_fib/0, test_readwrite/0, test_readonly/0, test_mixedload/0]).

% Fibonacci
fib(0) -> 1;
fib(1) -> 1;
fib(N) -> fib(N - 1) + fib(N - 2).

%% Benchmark helpers

% Run a benchmark function `Fun`, `Times` times, and print the time it took.
%
% `Name` is the name of the benchmark, used to identify the benchmark in the results.
%
% Recommendation: run each test at least 30 times to get statistically relevant
% results.
run_benchmark(Name, Fun, Times) ->
    ThisPid = self(),
    lists:foreach(fun (I) ->
        % Recommendation: to make the test fair, each run executes in its own,
        % newly created Erlang process. Otherwise, if all tests run in the same
        % process, the later tests start out with larger heap sizes and
        % therefore probably do fewer garbage collections. Also consider
        % restarting the Erlang emulator between each test.
        % Source: http://erlang.org/doc/efficiency_guide/profiling.html
        spawn_link(fun () ->
            measure(Name, Fun, I),
            ThisPid ! done
        end),
        receive done ->
            ok
        end
    end, lists:seq(1, Times)).

% Run the function `Fun` and print the time it took.
%
% `Name` is the name of the benchmark, used to identify the benchmark in the results.
% `I` is the index of the benchmark run, used to identify the run in the results.
measure(Name, Fun, I) ->
    io:format("Starting benchmark ~s: ~p~n", [Name, I]),

    % Start timers
    % Tips:
    % * Wall clock time measures the actual time spent on the benchmark.
    %   I/O, swapping, and other activities in the operating system kernel are
    %   included in the measurements. This can lead to larger variations.
    %   os:timestamp() is more precise (microseconds) than
    %   statistics(wall_clock) (milliseconds)
    % * CPU time measures the actual time spent on this program, summed for all
    %   threads. Time spent in the operating system kernel (such as swapping and
    %   I/O) is not included. This leads to smaller variations but is
    %   misleading.
    StartTime = os:timestamp(), % Wall clock time
    %statistics(runtime),       % CPU time, summed for all threads

    % Run
    Fun(),

    % Get and print statistics
    % Recommendation [1]:
    % The granularity of both measurement types can be high. Therefore, ensure
    % that each individual measurement lasts for at least several seconds.
    % [1] http://erlang.org/doc/efficiency_guide/profiling.html
    WallClockTime = timer:now_diff(os:timestamp(), StartTime),
    %{_, CpuTime} = statistics(runtime),
    io:format("Wall clock time = ~p ms~n", [WallClockTime / 1000.0]),
    %io:format("CPU time = ~p ms~n", [CpuTime]),
    io:format("~s done~n", [Name]).

%% Benchmarks
% Below are some example benchmarks. Extend these to test the best and worst
% case of your implementation, some typical scenarios you imagine, or some
% extreme scenarios.

test_fib() ->
    io:format("Parameters:~n"),
    io:format("~n"),
    run_benchmark("fib", fun test_fib_benchmark/0, 30).

test_fib_benchmark() ->
    % Spawn 64 processes that each compute the 30th Fibonacci number.
    BenchmarkPid = self(),
    Pids = [spawn(fun () ->
        fib(30),
        BenchmarkPid ! done
    end) || _ <- lists:seq(1, 64)],
    % Then we wait for all the processes to finish.
    lists:foreach(fun (_) ->
        receive done -> ok end
    end, Pids).

% Creates a server with 1000 buckets each containing 100 keys and values.
%
% This is used to initialize a server that already contains a lot of data, to then
% benchmark the performance of subsequent operations.
%
% Note that this code depends on the implementation of the server. You will need to
% change it if you change the representation of the data in the server.
initialize_server() ->
    % Seed random number generator to get reproducible results.
    rand:seed_s(exsplus, {0, 0, 0}),
    % Parameters
    NumberOfBuckets = 1000,
    NumberOfKeysPerBucket = 100,
    NumberOfKeysTotal = NumberOfBuckets * NumberOfKeysPerBucket,
    io:format("Parameters:~n"),
    io:format("Number of buckets: ~p~n", [NumberOfBuckets]),
    io:format("Number of keys per bucket: ~p~n", [NumberOfKeysPerBucket]),
    io:format("Number of keys total: ~p~n", [NumberOfKeysTotal]),
    io:format("~n"),
    % Generate bucket names: just the numbers from 1 to NumberOfBuckets.
    BucketNames = lists:seq(1, NumberOfBuckets),
    % Generate keys: just the numbers from 1 to NumberOfKeysPerBucket.
    % In this example, each bucket will contain the same keys. This is not
    % generally the case. It may be useful to experiment with buckets of
    % differing sizes.
    Keys = lists:seq(1, NumberOfKeysPerBucket),
    % Generate buckets dict.
    Buckets = dict:from_list(lists:map(fun (BucketName) ->
        {BucketName, dict:from_list(lists:map(fun (Key) ->
            {Key, generate_value(Key)}
        end, Keys))}
    end, BucketNames)),
    ServerPid = server_centralized:initialize_with(Buckets),
    {ServerPid, BucketNames, Keys}.

% Pick a random element from a list.
pick_random(List) ->
    lists:nth(rand:uniform(length(List)), List).

% Pick a random bucket name and key from the list of bucket names and keys.
pick_random_bucket_and_key(BucketNames, Keys) ->
    {pick_random(BucketNames), pick_random(Keys)}.

% Generate a value for `Key`. This is kept 'readable' on purpose, for easier debugging.
generate_value(Key) ->
    "Value for " ++ integer_to_list(Key).

% Test read-write operations.
%
% We will spawn 100 processes, each of which will do 1000 store and 1000 retrieve
% operations.
%
% Hence, test will do 50% store and 50% retrieve operations.
test_readwrite() ->
    {ServerPid, BucketNames, Keys} = initialize_server(),
    NumberOfClients = 100,
    NumberOfOperations = 1000,
    run_benchmark("readwrite",
        fun () ->
            BenchmarkPid = self(),
            % We spawn all the processes in parallel.
            Pids = [spawn(fun () ->
                lists:foreach(fun (_) ->
                    {BucketName, Key} = pick_random_bucket_and_key(BucketNames, Keys),
                    server:store(ServerPid, BucketName, Key, generate_value(Key))
                end, lists:seq(1, NumberOfOperations)),
                lists:foreach(fun (_) ->
                    {BucketName, Key} = pick_random_bucket_and_key(BucketNames, Keys),
                    server:retrieve(ServerPid, BucketName, Key)
                end, lists:seq(1, NumberOfOperations)),
                BenchmarkPid ! done
            end) || _ <- lists:seq(1, NumberOfClients)],
            % Then we wait for all the processes to finish.
            lists:foreach(fun (_) -> receive done -> ok end end, Pids)
        end,
        30).

% Test read-only operations.
%
% We will spawn 100 processes, each of which will do 1000 retrieve operations.
test_readonly() ->
    {ServerPid, BucketNames, Keys} = initialize_server(),
    NumberOfClients = 100,
    NumberOfOperations = 1000,
    run_benchmark("readonly",
        fun () ->
            BenchmarkPid = self(),
            % We spawn all the processes in parallel.
            Pids = [spawn(fun () ->
                lists:foreach(fun (_) ->
                    {BucketName, Key} = pick_random_bucket_and_key(BucketNames, Keys),
                    server:retrieve(ServerPid, BucketName, Key)
                end, lists:seq(1, NumberOfOperations)),
                BenchmarkPid ! done
            end) || _ <- lists:seq(1, NumberOfClients)],
            % Then we wait for all the processes to finish.
            lists:foreach(fun (_) -> receive done -> ok end end, Pids)
        end,
        30).

% Test a load of 99% retrieve and 1% store operations.
%
% We will spawn 100 processes, each of which will do 990 retrieve and 10 store
% operations.
test_mixedload() ->
    {ServerPid, BucketNames, Keys} = initialize_server(),
    NumberOfClients = 100,
    NumberOfWriteOperations = 10,
    NumberOfReadOperations = 990,
    run_benchmark("mixedload",
        fun () ->
            BenchmarkPid = self(),
            % We spawn all the processes in parallel.
            Pids = [spawn(fun () ->
                lists:foreach(fun (_) ->
                    {BucketName, Key} = pick_random_bucket_and_key(BucketNames, Keys),
                    server:store(ServerPid, BucketName, Key, generate_value(Key))
                end, lists:seq(1, NumberOfWriteOperations)),
                lists:foreach(fun (_) ->
                    {BucketName, Key} = pick_random_bucket_and_key(BucketNames, Keys),
                    server:retrieve(ServerPid, BucketName, Key)
                end, lists:seq(1, NumberOfReadOperations)),
                BenchmarkPid ! done
            end) || _ <- lists:seq(1, NumberOfClients)],
            % Then we wait for all the processes to finish.
            lists:foreach(fun (_) -> receive done -> ok end end, Pids)
        end,
        30).

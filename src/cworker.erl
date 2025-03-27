-module(cworker).

-export([new/2, worker/2]).

new(ServerPid, Buckets) ->
    if is_map(Buckets) ->
           spawn(?MODULE, worker, [ServerPid, Buckets]);
       true ->
           io:format("Unknown datastructure for cworker buckets, assuming\n      "
                     "               dict"),
           MapsBuckets =
               dict:fold(fun(Key, Value, AccIn) -> maps:put(Key, Value, AccIn) end,
                         maps:new(),
                         Buckets),
           spawn(?MODULE, worker, [ServerPid, MapsBuckets])
    end.

worker(Server, Buckets) ->
    receive
        {Client, disconnect} ->
            Server ! {self(), disconnect},
            Client ! {self(), disconnected};
        {_, replicate, ReplicaName, ReplicaBucket} ->
            worker(Server, maps:put(ReplicaName, ReplicaBucket, Buckets));
        {Client, create, BucketName} ->
            Bucket = cbucket:new(BucketName, maps:new()),
            Server ! {self(), replicate, BucketName, Bucket},
            Client ! {self(), created, BucketName},
            worker(Server, maps:put(BucketName, Bucket, Buckets));
        {Client, store, BucketName, Key, Value} ->
            case maps:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, store, Key, Value},
                    cworker(Server, Buckets, BucketName, Bucket);
                error ->
                    Store = maps:from_list([{Key, Value}]),
                    Bucket = cbucket:new(BucketName, Store),
                    Server ! {self(), replicate, BucketName, Bucket},
                    Client ! {self(), stored, BucketName, Key},
                    worker(Server, maps:put(BucketName, Bucket, Buckets))
            end;
        {Client, retrieve, BucketName, Key} ->
            case maps:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key},
                    cworker(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    worker(Server, Buckets)
            end;
        {Client, delete, BucketName, Key} ->
            case maps:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, delete, Key},
                    cworker(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    worker(Server, Buckets)
            end
    end.

cworker(Server, Buckets, CachedBucketName, CachedBucket) ->
    receive
        {Client, disconnect} ->
            Server ! {self(), disconnect},
            Client ! {self(), disconnected};
        {_, replicate, ReplicaName, ReplicaBucket} ->
            NewBuckets = maps:put(ReplicaName, ReplicaBucket, Buckets),
            cworker(Server, NewBuckets, CachedBucketName, CachedBucket);
        {Client, create, BucketName} ->
            Bucket = cbucket:new(BucketName, maps:new()),
            Server ! {self(), replicate, BucketName, Bucket},
            Client ! {self(), created, BucketName},
            cworker(Server, maps:put(BucketName, Bucket, Buckets), CachedBucketName, CachedBucket);
        {Client, Operation, BucketName, Key, Value} when BucketName == CachedBucketName ->
            CachedBucket ! {Client, Operation, Key, Value},
            cworker(Server, Buckets, CachedBucketName, CachedBucket);
        {Client, store, BucketName, Key, Value} ->
            case maps:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, store, Key, Value},
                    cworker(Server, Buckets, BucketName, Bucket);
                error ->
                    Store = maps:from_list([{Key, Value}]),
                    Bucket = cbucket:new(BucketName, Store),
                    Server ! {self(), replicate, BucketName, Bucket},
                    Client ! {self(), stored, BucketName, Key},
                    cworker(Server, maps:put(Bucket, Buckets), CachedBucketName, CachedBucket)
            end;
        {Client, retrieve, BucketName, Key} ->
            case maps:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key},
                    cworker(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    cworker(Server, Buckets, CachedBucketName, CachedBucket)
            end;
        {Client, delete, BucketName, Key} ->
            case maps:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, delete, Key},
                    cworker(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    cworker(Server, Buckets, CachedBucketName, CachedBucket)
            end
    end.

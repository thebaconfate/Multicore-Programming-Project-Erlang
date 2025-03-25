-module(cworker).

-export([new/2, worker/2]).

new(ServerPid, Bucket) ->
    spawn(?MODULE, worker, [ServerPid, Bucket]).

worker(Server, Buckets) ->
    receive
        {Client, disconnect} ->
            Server ! {self(), disconnect},
            Client ! {self(), disconnected};
        {_, replicate, ReplicaName, ReplicaBucket} ->
            worker(Server, dict:store(ReplicaName, ReplicaBucket, Buckets));
        {Client, create, BucketName} ->
            Bucket = cbucket:new(BucketName, dict:new()),
            Server ! {self(), replicate, BucketName, Bucket},
            Client ! {self(), created, BucketName},
            worker(Server, dict:store(BucketName, Bucket, Buckets));
        {Client, store, BucketName, Key, Value} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, store, Key, Value},
                    cworker(Server, Buckets, BucketName, Bucket);
                error ->
                    Store = dict:from_list([{Key, Value}]),
                    Bucket = cbucket:new(BucketName, Store),
                    Server ! {self(), replicate, BucketName, Bucket},
                    Client ! {self(), stored, BucketName, Key},
                    worker(Server, Buckets)
            end;
        {Client, retrieve, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key},
                    cworker(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    worker(Server, Buckets)
            end;
        {Client, delete, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
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
            NewBuckets = dict:store(ReplicaName, ReplicaBucket, Buckets),
            cworker(Server, NewBuckets, CachedBucketName, CachedBucket);
        {Client, create, BucketName} ->
            Bucket = cbucket:new(BucketName, dict:new()),
            Server ! {self(), replicate, BucketName, Bucket},
            Client ! {self(), created, BucketName},
            cworker(Server,
                    dict:store(BucketName, Bucket, Buckets),
                    CachedBucketName,
                    CachedBucket);
        {Client, Operation, BucketName, Key, Value} when BucketName == CachedBucketName ->
            CachedBucket ! {Client, Operation, Key, Value},
            cworker(Server, Buckets, CachedBucketName, CachedBucket);
        {Client, store, BucketName, Key, Value} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, store, Key, Value},
                    cworker(Server, Buckets, BucketName, Bucket);
                error ->
                    Store = dict:from_list([{Key, Value}]),
                    Bucket = cbucket:new(BucketName, Store),
                    Server ! {self(), replicate, BucketName, Bucket},
                    Client ! {self(), stored, BucketName, Key},
                    cworker(Server, dict:store(Bucket, Buckets), CachedBucketName, CachedBucket)
            end;
        {Client, retrieve, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key},
                    cworker(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    cworker(Server, Buckets, CachedBucketName, CachedBucket)
            end;
        {Client, delete, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, delete, Key},
                    cworker(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    cworker(Server, Buckets, CachedBucketName, CachedBucket)
            end
    end.

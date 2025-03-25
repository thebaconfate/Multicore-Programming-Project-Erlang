-module(worker).

-export([new/2, worker/2]).

new(ServerPid, BucketPid) ->
    spawn(?MODULE, client_actor, [ServerPid, BucketPid]).

worker(Server, Buckets) ->
    receive
        {Client, disconnect} ->
            Server ! {self(), disconnect},
            Client ! {self(), disconnected};
        {_, replicate, ReplicaName, ReplicaBucket} ->
            NewBuckets = dict:store(ReplicaName, ReplicaBucket, Buckets),
            worker(Server, NewBuckets);
        {Client, create, BucketName} ->
            Bucket = bucket:new(BucketName, dict:new()),
            NewBuckets = dict:store(BucketName, Bucket, Buckets),
            Server ! {self(), replicate, BucketName, Bucket},
            Client ! {self(), created, BucketName},
            worker(Server, NewBuckets);
        {Client, store, BucketName, Key, Value} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, store, Key, Value};
                error ->
                    Client ! {self(), not_found, BucketName, Key}
            end,
            worker(Server, Buckets);
        {Client, retrieve, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key},
                    cached_worker(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    worker(Server, Buckets)
            end;
        {Client, delete, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, delete, Key},
                    cached_worker(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    worker(Server, Buckets)
            end
    end.

cached_worker(Server, Buckets, CachedBucketName, CachedBucket) ->
    receive
        {Client, disconnect} ->
            Server ! {self(), disconnect},
            Client ! {self(), disconnected};
        {_, replicate, ReplicaName, ReplicaBucket} ->
            NewBuckets = dict:store(ReplicaName, ReplicaBucket, Buckets),
            cached_worker(Server, NewBuckets, CachedBucketName, CachedBucket);
        {Client, create, BucketName} ->
            Bucket = bucket:new(BucketName, dict:new()),
            NewBuckets = dict:store(BucketName, Bucket, Buckets),
            Server ! {self(), replicate, BucketName, Bucket},
            Client ! {self(), created, BucketName},
            cached_worker(Server, NewBuckets, CachedBucketName, CachedBucket);
        {Client, store, BucketName, Key, Value} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, store, Key, Value},
                    cached_worker(Server, Buckets, BucketName, Bucket);
                error ->
                    Store = dict:from_list([{Key, Value}]),
                    Bucket = bucket:new(BucketName, Store),
                    Server ! {self(), replicate, BucketName, Bucket},
                    Client ! {self(), stored, BucketName, Key},
                    cached_worker(Server,
                                  dict:store(Bucket, Buckets),
                                  CachedBucketName,
                                  CachedBucket)
            end;
        {Client, retrieve, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key},
                    cached_worker(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    cached_worker(Server, Buckets, CachedBucketName, CachedBucket)
            end;
        {Client, delete, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, delete, Key},
                    cached_worker(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    cached_worker(Server, Buckets, CachedBucketName, CachedBucket)
            end
    end.

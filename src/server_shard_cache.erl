-module(server_shard_cache).

-export([initialize/0, initialize_with/1, server/1]).

initialize() ->
    initialize_with(dict:new()).

initialize_with(InitBuckets) ->
    Buckets = initialize_buckets(InitBuckets),
    ServerPid = server(Buckets),
    catch unregister(server_actor),
    register(server_actor, ServerPid),
    ServerPid.

initialize_buckets(InitBuckets) ->
    dict:fold(fun(BucketName, Bucket, AccIn) ->
                 dict:store(BucketName, bucket:new(BucketName, Bucket), AccIn)
              end,
              dict:new(),
              InitBuckets).

server(Buckets) ->
    receive
        {Client, connect} ->
            Client ! {self(), connected},
            server(Buckets);
        {Client, disconnect} ->
            Client ! {self(), disconnected},
            server(Buckets);
        {Client, store, BucketName, Key, Value} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, store, Key, Value},
                    cserver(Buckets, BucketName, Bucket);
                error ->
                    Store = dict:from_list({Key, Value}),
                    NewBucket = bucket:new(BucketName, Store),
                    Client ! {self(), stored, BucketName, Key},
                    server(dict:store(BucketName, NewBucket, Buckets))
            end;
        {Client, retrieve, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key},
                    cserver(Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    server(Buckets)
            end;
        {Client, delete, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, delete, Key},
                    cserver(Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    server(Buckets)
            end
    end.

cserver(Buckets, CachedBucketName, CachedBucket) ->
    receive
        {Client, connect} ->
            Client ! {self(), connected},
            cserver(Buckets, CachedBucketName, CachedBucket);
        {Client, disconnect} ->
            Client ! {self(), disconnected},
            cserver(Buckets, CachedBucketName, CachedBucket);
        {Client, store, BucketName, Key, Value} when BucketName == CachedBucketName ->
            CachedBucket ! {Client, store, Key, Value},
            cserver(Buckets, CachedBucketName, CachedBucket);
        {Client, store, BucketName, Key, Value} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, store, Key, Value},
                    cserver(Buckets, BucketName, Bucket);
                error ->
                    Store = dict:from_list({Key, Value}),
                    NewBucket = bucket:new(BucketName, Store),
                    Client ! {self(), stored, BucketName, Key},
                    cserver(dict:store(BucketName, NewBucket, Buckets),
                            CachedBucketName,
                            CachedBucket)
            end;
        {Client, retrieve, BucketName, Key} when BucketName == CachedBucketName ->
            CachedBucket ! {Client, retrieve, Key},
            cserver(Buckets, CachedBucketName, CachedBucket);
        {Client, retrieve, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key};
                error ->
                    Client ! {self(), not_found, BucketName, Key}
            end,
            server(Buckets);
        {Client, delete, BucketName, Key} ->
            case dict:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, delete, Key};
                error ->
                    Client ! {self(), not_found, BucketName, Key}
            end
    end.

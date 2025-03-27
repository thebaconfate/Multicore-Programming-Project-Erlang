-module(server_shard_cache).

-export([initialize/0, initialize_with/1, server/1]).

initialize() ->
    initialize_with(dict:new()).

initialize_with(InitBuckets) ->
    Buckets = initialize_cbuckets(InitBuckets),
    ServerPid = spawn_link(?MODULE, server, [Buckets]),
    catch unregister(server),
    register(server, ServerPid),
    ServerPid.

initialize_cbuckets(InitBuckets) ->
    dict:fold(fun(BucketName, Bucket, AccIn) ->
                 maps:put(BucketName,
                          cbucket:new(BucketName,
                                      dict:fold(fun(Key, Value, _AccIn) ->
                                                   maps:put(Key, Value, _AccIn)
                                                end,
                                                maps:new(),
                                                Bucket)),
                          AccIn)
              end,
              maps:new(),
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
            case maps:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, store, Key, Value},
                    cserver(Buckets, BucketName, Bucket);
                error ->
                    Store = maps:from_list({Key, Value}),
                    NewBucket = cbucket:new(BucketName, Store),
                    Client ! {self(), stored, BucketName, Key},
                    server(maps:put(BucketName, NewBucket, Buckets))
            end;
        {Client, retrieve, BucketName, Key} ->
            case maps:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key},
                    cserver(Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    server(Buckets)
            end;
        {Client, delete, BucketName, Key} ->
            case maps:find(BucketName, Buckets) of
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
            case maps:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, store, Key, Value},
                    cserver(Buckets, BucketName, Bucket);
                error ->
                    Store = maps:from_list({Key, Value}),
                    NewBucket = cbucket:new(BucketName, Store),
                    Client ! {self(), stored, BucketName, Key},
                    cserver(maps:put(BucketName, NewBucket, Buckets),
                            CachedBucketName,
                            CachedBucket)
            end;
        {Client, retrieve, BucketName, Key} when BucketName == CachedBucketName ->
            CachedBucket ! {Client, retrieve, Key},
            cserver(Buckets, CachedBucketName, CachedBucket);
        {Client, retrieve, BucketName, Key} ->
            case maps:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key};
                error ->
                    Client ! {self(), not_found, BucketName, Key}
            end,
            server(Buckets);
        {Client, delete, BucketName, Key} ->
            case maps:find(BucketName, Buckets) of
                {ok, Bucket} ->
                    Bucket ! {Client, delete, Key},
                    cserver(Buckets, BucketName, Buckets);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    cserver(Buckets, CachedBucketName, CachedBucket)
            end
    end.

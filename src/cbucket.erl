-module(cbucket).

-export([new/2, bucket/2, cbucket/4]).

new(Name, Store) ->
    if is_map(Store) ->
           spawn(?MODULE, bucket, [Name, Store]);
       true ->
           io:format("Unknown store datastructure for cbucket initialization, assuming "
                     "dict ~n"),
           MapStore =
               dict:fold(fun(Key, Value, AccIn) -> maps:put(Key, Value, AccIn) end,
                         maps:new(),
                         Store),
           spawn(?MODULE, bucket, [Name, MapStore])
    end.

bucket(BucketName, Store) ->
    receive
        {Sender, store, Key, Value} ->
            NewStore = maps:put(Key, Value, Store),
            Sender ! {self(), stored, BucketName, Key},
            bucket(BucketName, NewStore);
        {Sender, retrieve, Key} ->
            case maps:find(Key, Store) of
                {ok, Value} ->
                    Sender ! {self(), retrieved, BucketName, Key, Value},
                    cbucket(BucketName, Store, Key, Value);
                error ->
                    Sender ! {self(), not_found, BucketName, Key},
                    bucket(BucketName, Store)
            end;
        {Sender, delete, Key} ->
            NewStore = maps:remove(Key, Store),
            Sender ! {self(), deleted, BucketName, Key},
            bucket(BucketName, NewStore)
    end.

cbucket(BucketName, Store, CachedKey, CachedValue) ->
    receive
        {Sender, store, Key, Value} ->
            Sender ! {self(), stored, BucketName, Key},
            cbucket(BucketName, maps:put(Key, Value, Store), CachedKey, CachedValue);
        {Sender, retrieve, Key} when CachedKey == Key ->
            Sender ! {self(), retrieved, BucketName, CachedKey, CachedValue},
            cbucket(BucketName, Store, CachedKey, CachedValue);
        {Sender, retrieve, Key} ->
            case maps:find(Key, Store) of
                {ok, Value} ->
                    Sender ! {self(), retrieved, BucketName, Key, Value},
                    cbucket(BucketName, Store, Key, Value);
                error ->
                    Sender ! {self(), not_found, BucketName, Key},
                    cbucket(BucketName, Store, CachedKey, CachedValue)
            end;
        {Sender, delete, Key} when CachedKey == Key ->
            NewStore = maps:remove(Key, Store),
            Sender ! {self(), deleted, BucketName, Key},
            bucket(BucketName, NewStore);
        {Sender, delete, Key} ->
            NewStore = maps:remove(Key, Store),
            Sender ! {self(), deleted, BucketName, Key},
            cbucket(BucketName, NewStore, CachedKey, CachedValue)
    end.

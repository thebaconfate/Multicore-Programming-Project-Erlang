-module(cbucket).

-export([new/2, bucket/2, cached_bucket/4]).

new(Name, Store) ->
    spawn(?MODULE, bucket, [Name, Store]).

bucket(BucketName, Store) ->
    receive
        {Sender, store, Key, Value} ->
            NewStore = dict:store(Key, Value, Store),
            Sender ! {self(), stored, BucketName, Key},
            bucket(BucketName, NewStore);
        {Sender, retrieve, Key} ->
            case dict:find(Key, Store) of
                {ok, Value} ->
                    Sender ! {self(), retrieved, BucketName, Key, Value},
                    cached_bucket(BucketName, Store, Key, Value);
                error ->
                    Sender ! {self(), not_found, BucketName, Key},
                    bucket(BucketName, Store)
            end;
        {Sender, delete, Key} ->
            NewStore = dict:erase(Key, Store),
            Sender ! {self(), deleted, BucketName, Key},
            bucket(BucketName, NewStore)
    end.

cached_bucket(BucketName, Store, CachedKey, CachedValue) ->
    receive
        {Sender, store, Key, Value} ->
            Sender ! {self(), stored, BucketName, Key},
            cached_bucket(BucketName, dict:store(Key, Value, Store), CachedKey, CachedValue);
        {Sender, retrieve, Key} when CachedKey == Key ->
            Sender ! {self(), retrieved, BucketName, CachedKey, CachedValue},
            cached_bucket(BucketName, Store, CachedKey, CachedValue);
        {Sender, retrieve, Key} ->
            case dict:find(Key, Store) of
                {ok, Value} ->
                    Sender ! {self(), retrieved, BucketName, Key, Value},
                    cached_bucket(BucketName, Store, Key, Value);
                error ->
                    Sender ! {self(), not_found, BucketName, Key},
                    cached_bucket(BucketName, Store, CachedKey, CachedValue)
            end;
        {Sender, delete, Key} when CachedKey == Key ->
            NewStore = dict:erase(Key, Store),
            Sender ! {self(), deleted, BucketName, Key},
            bucket(BucketName, NewStore);
        {Sender, delete, Key} ->
            NewStore = dict:erase(Key, Store),
            Sender ! {self(), deleted, BucketName, Key},
            cached_bucket(BucketName, NewStore, CachedKey, CachedValue)
    end.

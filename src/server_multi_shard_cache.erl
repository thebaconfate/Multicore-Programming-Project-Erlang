-module(server_multi_shard_cache).

-include_lib("eunit/include/eunit.hrl").

-export([initialize/0, initialize_with/1, server_actor/2, client_actor/2, bucket_actor/2,
         initialize_test/0, connect_test/0, disconnect_test/0, create_test/0, store_test/0,
         delete_test/0, multiple_clients_test/0, non_empty_dict_test/0]).

find_key(Dict, Key) ->
    dict:find(Key, Dict).

store_key(Dict, Key, Value) ->
    dict:store(Key, Value, Dict).

delete_key(Dict, Key) ->
    dict:erase(Key, Dict).

initialize() ->
    initialize_with(dict:new()).

initialize_with(InitBuckets) ->
    Buckets = initialize_buckets(InitBuckets),
    ServerPid = spawn_link(?MODULE, server_actor, [[], Buckets]),
    catch unregister(server_actor),
    register(server_actor, ServerPid),
    ServerPid.

initialize_buckets(InitBuckets) ->
    io:format("Converting initbuckets"),
    dict:fold(fun(BucketName, Store, AccIn) ->
                 Bucket = new_bucket(BucketName, Store),
                 dict:store(BucketName, Bucket, AccIn)
              end,
              dict:new(),
              InitBuckets).

server_actor(Clients, Buckets) ->
    receive
        {Client, replicate, ReplicaName, ReplicaBucket} ->
            lists:foreach(fun(OtherClient) ->
                             if OtherClient /= Client ->
                                    OtherClient ! {self(), replicate, ReplicaName, ReplicaBucket};
                                true -> true
                             end
                          end,
                          Clients),
            NewBuckets = store_key(Buckets, ReplicaName, ReplicaBucket),
            server_actor(Clients, NewBuckets);
        {Sender, connect} ->
            NewClient = new_client(self(), Buckets),
            Sender ! {NewClient, connected},
            server_actor([NewClient | Clients], Buckets);
        {Sender, disconnect} ->
            NewClients = Clients -- [Sender],
            server_actor(NewClients, Buckets)
    end.

new_client(ServerPid, Buckets) ->
    spawn(?MODULE, client_actor, [ServerPid, Buckets]).

client_actor(Server, Buckets) ->
    receive
        {Client, disconnect} ->
            Server ! {self(), disconnect},
            Client ! {self(), disconnected};
        {_, replicate, ReplicaName, ReplicaBucket} ->
            NewBuckets = store_key(Buckets, ReplicaName, ReplicaBucket),
            client_actor(Server, NewBuckets);
        {Client, create, BucketName} ->
            Bucket = new_bucket(BucketName, dict:new()),
            NewBuckets = store_key(Buckets, BucketName, Bucket),
            Server ! {self(), replicate, BucketName, Bucket},
            Client ! {self(), created, BucketName},
            client_actor(Server, NewBuckets);
        {Client, store, BucketName, Key, Value} ->
            case find_key(Buckets, BucketName) of
                {ok, Bucket} ->
                    Bucket ! {Client, store, Key, Value};
                error ->
                    Client ! {self(), not_found, BucketName, Key}
            end,
            client_actor(Server, Buckets);
        {Client, retrieve, BucketName, Key} ->
            case find_key(Buckets, BucketName) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key},
                    client_actor(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    client_actor(Server, Buckets)
            end;
        {Client, delete, BucketName, Key} ->
            case find_key(Buckets, BucketName) of
                {ok, Bucket} ->
                    Bucket ! {Client, delete, Key},
                    client_actor(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    client_actor(Server, Buckets)
            end
    end.

client_actor(Server, Buckets, CachedBucketName, CachedBucket) ->
    receive
        {Client, disconnect} ->
            Server ! {self(), disconnect},
            Client ! {self(), disconnected};
        {_, replicate, ReplicaName, ReplicaBucket} ->
            NewBuckets = store_key(Buckets, ReplicaName, ReplicaBucket),
            client_actor(Server, NewBuckets, CachedBucketName, CachedBucket);
        {Client, create, BucketName} ->
            Bucket = new_bucket(BucketName, dict:new()),
            NewBuckets = store_key(Buckets, BucketName, Bucket),
            Server ! {self(), replicate, BucketName, Bucket},
            Client ! {self(), created, BucketName},
            client_actor(Server, NewBuckets, CachedBucketName, CachedBucket);
        {Client, store, BucketName, Key, Value} ->
            Bucket = find_key(Buckets, BucketName),
            Bucket ! {Client, store, Key, Value},
            client_actor(Server, Buckets, CachedBucketName, CachedBucket);
        {Client, retrieve, BucketName, Key} ->
            case find_key(Buckets, BucketName) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key},
                    client_actor(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    client_actor(Server, Buckets, CachedBucketName, CachedBucket)
            end;
        {Client, delete, BucketName, Key} ->
            case find_key(Buckets, BucketName) of
                {ok, Bucket} ->
                    Bucket ! {Client, delete, Key},
                    client_actor(Server, Buckets, BucketName, Bucket);
                error ->
                    Client ! {self(), not_found, BucketName, Key},
                    client_actor(Server, Buckets, CachedBucketName, CachedBucket)
            end
    end.

new_bucket(Name, Store) ->
    spawn(?MODULE, bucket_actor, [Name, Store]).

bucket_actor(BucketName, Store) ->
    receive
        {Sender, store, Key, Value} ->
            NewStore = store_key(Store, Key, Value),
            Sender ! {self(), stored, BucketName, Key},
            bucket_actor(BucketName, NewStore);
        {Sender, retrieve, Key} ->
            case find_key(Store, Key) of
                {ok, Value} ->
                    Sender ! {self(), retrieved, BucketName, Key, Value},
                    bucket_actor(BucketName, Store, Key, Value);
                error ->
                    Sender ! {self(), not_found, BucketName, Key},
                    bucket_actor(BucketName, Store)
            end;
        {Sender, delete, Key} ->
            NewStore = delete_key(Store, Key),
            Sender ! {self(), deleted, BucketName, Key},
            bucket_actor(BucketName, NewStore)
    end.

bucket_actor(BucketName, Store, CachedKey, CachedValue) ->
    receive
        {Sender, store, Key, Value} ->
            NewStore = store_key(Store, Key, Value),
            Sender ! {self(), stored, BucketName, Key},
            bucket_actor(BucketName, NewStore, CachedKey, CachedValue);
        {Sender, retrieve, Key} when CachedKey == Key ->
            Sender ! {self(), retrieved, BucketName, CachedKey, CachedValue},
            bucket_actor(BucketName, Store, CachedKey, CachedValue);
        {Sender, retrieve, Key} ->
            case find_key(Store, Key) of
                {ok, Value} ->
                    Sender ! {self(), retrieved, BucketName, Key, Value},
                    bucket_actor(BucketName, Store, Key, Value);
                error ->
                    Sender ! {self(), not_found, BucketName, Key},
                    bucket_actor(BucketName, Store, CachedKey, CachedValue)
            end;
        {Sender, delete, Key} when CachedKey == Key ->
            NewStore = delete_key(Store, Key),
            Sender ! {self(), deleted, BucketName, Key},
            bucket_actor(BucketName, NewStore);
        {Sender, delete, Key} ->
            NewStore = delete_key(Store, Key),
            Sender ! {self(), deleted, BucketName, Key},
            bucket_actor(BucketName, NewStore, CachedKey, CachedValue)
    end.

initialize_test() ->
    catch unregister(server_actor),
    initialize().

initialize_test_with(Buckets) ->
    catch unregister(server_actor),
    initialize_with(Buckets).

% Test connect function.
connect_test() ->
    ServerPid = initialize_test(),
    Result = server:connect(ServerPid),
    case Result of
        {ClientServerPid, connected} ->
            ?assertMatch({ClientServerPid, connected}, Result),
            ClientServerPid;
        _ ->
            ?assertMatch({_, connected}, Result)
    end.

% Test disconnect function.
disconnect_test() ->
    ServerPid = connect_test(),
    ?assertMatch({_, disconnected}, server:disconnect(ServerPid)),
    ServerPid.

% Test create function.
create_test() ->
    ServerPid = connect_test(),
    ?assertMatch({_, created, "shopping"}, server:create(ServerPid, "shopping")),
    ServerPid.

% Test store function.
store_test() ->
    ServerPid = create_test(),
    ?assertMatch({_, stored, "shopping", "milk"},
                 server:store(ServerPid, "shopping", "milk", 1)),
    ?assertMatch({_, stored, "shopping", "eggs"},
                 server:store(ServerPid, "shopping", "eggs", 3)),
    ServerPid.

% Test retrieve function.
retrieve_test() ->
    ServerPid = store_test(),
    ?assertMatch({_, retrieved, "shopping", "milk", 1},
                 server:retrieve(ServerPid, "shopping", "milk")),
    ?assertMatch({_, retrieved, "shopping", "eggs", 3},
                 server:retrieve(ServerPid, "shopping", "eggs")),
    ?assertMatch({_, not_found, "shopping", "bread"},
                 server:retrieve(ServerPid, "shopping", "bread")),
    ServerPid.

% Test delete function.
delete_test() ->
    ServerPid = retrieve_test(),
    ?assertMatch({_, deleted, "shopping", "eggs"},
                 server:delete(ServerPid, "shopping", "eggs")),
    ?assertMatch({_, not_found, "shopping", "eggs"},
                 server:retrieve(ServerPid, "shopping", "eggs")),
    ServerPid.

non_empty_dict_test() ->
    NumberOfBuckets = 1000,
    NumberOfKeysPerBucket = 100,
    Seq = lists:seq(1, NumberOfKeysPerBucket),
    StoreAsList = lists:map(fun(Key) -> {Key, 1} end, Seq),
    Store = dict:from_list(StoreAsList),
    BucketsAsList = lists:map(fun(Key) -> {Key, Store} end, lists:seq(1, NumberOfBuckets)),
    Buckets = dict:from_list(BucketsAsList),
    initialize_with(Buckets).

multiple_clients_test() ->
    ServerPid = initialize_test(),
    [{Client1, connected}, {Client2, connected}] =
        [server:connect(ServerPid) || _ <- lists:seq(1, 2)],
    ?assertMatch({_, created, "shopping"}, server:create(Client1, "shopping")),
    timer:sleep(100),
    ?assertMatch({_, stored, "shopping", "milk"},
                 server:store(Client2, "shopping", "milk", 1)),
    ?assertMatch({_, retrieved, "shopping", "milk", 1},
                 server:retrieve(Client1, "shopping", "milk")),
    ?assertMatch({_, deleted, "shopping", "eggs"},
                 server:delete(Client1, "shopping", "eggs")),
    ?assertMatch({_, not_found, "shopping", "eggs"},
                 server:retrieve(Client2, "shopping", "eggs")).

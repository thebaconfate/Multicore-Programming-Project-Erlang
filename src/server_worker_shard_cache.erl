-module(server_worker_shard_cache).

-include_lib("eunit/include/eunit.hrl").

-export([initialize/0, initialize_with/1, server/2, initialize_test_with/1]).

initialize() ->
    initialize_with(dict:new()).

initialize_with(InitBuckets) ->
    Buckets = initialize_buckets(InitBuckets),
    ServerPid = spawn_link(?MODULE, server, [[], Buckets]),
    catch unregister(server),
    register(server, ServerPid),
    ServerPid.

initialize_buckets(InitBuckets) ->
    dict:fold(fun(BucketName, Store, AccIn) ->
                 dict:store(BucketName, bucket:new(BucketName, Store), AccIn)
              end,
              dict:new(),
              InitBuckets).

server(Clients, Buckets) ->
    receive
        {Client, replicate, ReplicaName, ReplicaBucket} ->
            lists:foreach(fun(OtherClient) ->
                             if OtherClient /= Client ->
                                    OtherClient ! {self(), replicate, ReplicaName, ReplicaBucket};
                                true -> true
                             end
                          end,
                          Clients),
            NewBuckets = dict:store(ReplicaName, ReplicaBucket, Buckets),
            server(Clients, NewBuckets);
        {Sender, connect} ->
            NewClient = cworker:new(self(), Buckets),
            Sender ! {NewClient, connected},
            server([NewClient | Clients], Buckets);
        {Sender, disconnect} ->
            NewClients = Clients -- [Sender],
            server(NewClients, Buckets)
    end.

initialize_test() ->
    catch unregister(server),
    initialize().

initialize_test_with(Buckets) ->
    catch unregister(server),
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

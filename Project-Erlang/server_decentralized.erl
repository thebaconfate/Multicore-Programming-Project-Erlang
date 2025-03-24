-module(server_decentralized).

-export([initialize/0, initialize_with/1, server_actor/2]).

find_key(Dict, Key) ->
    dict:find(Key, Dict).

store_key(Dict, Key, Value) ->
    dict:store(Key, Value, Dict).

delete_key(Dict, Key) ->
    dict:erase(Key, Dict).

initialize() ->
    initialize_with([]).

initialize_with(ClientList) ->
    ServerPid = spawn_link(?MODULE, server_actor, [ClientList]),
    catch unregister(server_actor),
    register(server_actor, ServerPid),
    ServerPid.

server_actor(Clients, Buckets) ->
    receive
        {Client, replicate, ReplicaName, ReplicaBucket} ->
            lists:foreach(fun(OtherClient) when OtherClient /= Client ->
                             OtherClient ! {self(), replicate, ReplicaName, ReplicaBucket}
                          end,
                          Clients),
            NewBuckets = store_key(Buckets, ReplicaName, ReplicaBucket),
            server_actor(Clients, NewBuckets)
    end,
    receive
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
            Client ! {self(), disconnect};
        {Client, create, BucketName} ->
            Bucket = new_bucket(BucketName),
            NewBuckets = store_key(Buckets, BucketName, Bucket),
            Server ! {self(), replicate, BucketName, Bucket},
            Client ! {self(), created, BucketName},
            client_actor(Server, NewBuckets);
        {Client, store, BucketName, Key, Value} ->
            Bucket = find_key(Buckets, BucketName),
            Bucket ! {Client, store, Key, Value},
            client_actor(Server, Buckets);
        {Client, retrieve, BucketName, Key} ->
            case find_key(Buckets, BucketName) of
                {ok, Bucket} ->
                    Bucket ! {Client, retrieve, Key};
                error ->
                    Client ! {self(), not_found, BucketName, Key}
            end,
            client_actor(Server, Buckets);
        {Client, delete, BucketName, Key} ->
            case find_key(Buckets, BucketName) of
                {ok, Bucket} ->
                    Bucket ! {Client, delete, Key};
                error ->
                    Client ! {self(), not_found, BucketName, Key}
            end
    end.

new_bucket(Name) ->
    spawn(?MODULE, bucket_actor, [Name, dict:new()]).

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
            Sender ! {self(), delete, BucketName, Key},
            bucket_actor(BucketName, NewStore);
        {Sender, delete, Key} ->
            NewStore = delete_key(Store, Key),
            Sender ! {self(), deleted, BucketName, Key},
            bucket_actor(BucketName, NewStore, CachedKey, CachedValue)
    end.

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

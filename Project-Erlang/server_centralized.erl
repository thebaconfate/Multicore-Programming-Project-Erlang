%% This is a simple implementation of the project, using one server process.
%%
%% It will create one "server" actor that contains all internal state (users,
%% their subscriptions, and their messages).
%%
%% This implementation is provided with unit tests, however, these tests are
%% neither complete nor implementation independent, so be careful when reusing
%% them.
-module(server_centralized).

-include_lib("eunit/include/eunit.hrl").

-export([initialize/0, initialize_with/1, server_actor/1, typical_session_1/1,
    typical_session_2/1]).

%%
%% Additional API Functions
%%

% Start server.
initialize() ->
    initialize_with(dict:new()).

% Start server with an initial state.
% Useful for benchmarking.
initialize_with(Buckets) ->
    ServerPid = spawn_link(?MODULE, server_actor, [Buckets]),
    catch unregister(server_actor),
    register(server_actor, ServerPid),
    ServerPid.

% The server actor works like a small database and encapsulates all state of
% this simple implementation.
%
% Buckets is a dictionary of bucket names, to a dictionary of keys to values.
server_actor(Buckets) ->
    receive
        {Sender, connect} ->
            % This doesn't do anything, but you could use this operation if needed.
            Sender ! {self(), connected},
            server_actor(Buckets);

        {Sender, disconnect} ->
            % This doesn't do anything, but you could use this operation if needed.
            Sender ! {self(), disconnected},
            server_actor(Buckets);

        {Sender, create, BucketName} ->
            % If a bucket already exists, it is overwritten with an empty bucket.
            % This is probably not what you want, but we disregard this error for
            % this project.
            NewBuckets = dict:store(BucketName, dict:new(), Buckets),
            Sender ! {self(), created, BucketName},
            server_actor(NewBuckets);

        {Sender, store, BucketName, Key, Value} ->
            % Existing keys are overwritten. This is the desired behavior.
            OldBucket = get_bucket(BucketName, Buckets),
            NewBucket = dict:store(Key, Value, OldBucket),
            NewBuckets = dict:store(BucketName, NewBucket, Buckets),
            Sender ! {self(), stored, BucketName, Key},
            server_actor(NewBuckets);

        {Sender, retrieve, BucketName, Key} ->
            Bucket = get_bucket(BucketName, Buckets),
            case dict:find(Key, Bucket) of
                {ok, Value} -> Sender ! {self(), retrieved, BucketName, Key, Value};
                error -> Sender ! {self(), not_found, BucketName, Key}
            end,
            server_actor(Buckets);

        {Sender, delete, BucketName, Key} ->
            OldBucket = get_bucket(BucketName, Buckets),
            NewBucket = dict:erase(Key, OldBucket),
            NewBuckets = dict:store(BucketName, NewBucket, Buckets),
            Sender ! {self(), deleted, BucketName, Key},
            server_actor(NewBuckets)
    end.

%%
%% Internal Functions
%%

% Find a bucket in the dictionary. If it doesn't exist, returns an empty dictionary.
get_bucket(BucketName, Buckets) ->
    case dict:find(BucketName, Buckets) of
        {ok, Bucket} -> Bucket;
        error -> dict:new()
    end.

%%
%% Tests
%%
% These tests are for this specific implementation. You can re-use them, but you might
% need to modify them.

% Test initialize function.
initialize_test() ->
    catch unregister(server_actor),
    initialize().

% Test connect function.
connect_test() ->
    ServerPid = initialize_test(),
    ?assertMatch({_, connected}, server:connect(ServerPid)),
    ServerPid.

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
    ?assertMatch({_, stored, "shopping", "milk"}, server:store(ServerPid, "shopping", "milk", 1)),
    ?assertMatch({_, stored, "shopping", "eggs"}, server:store(ServerPid, "shopping", "eggs", 3)),
    ServerPid.

% Test retrieve function.
retrieve_test() ->
    ServerPid = store_test(),
    ?assertMatch({_, retrieved, "shopping", "milk", 1}, server:retrieve(ServerPid, "shopping", "milk")),
    ?assertMatch({_, retrieved, "shopping", "eggs", 3}, server:retrieve(ServerPid, "shopping", "eggs")),
    ?assertMatch({_, not_found, "shopping", "bread"}, server:retrieve(ServerPid, "shopping", "bread")),
    ServerPid.

% Test delete function.
delete_test() ->
    ServerPid = retrieve_test(),
    ?assertMatch({_, deleted, "shopping", "eggs"}, server:delete(ServerPid, "shopping", "eggs")),
    ?assertMatch({_, not_found, "shopping", "eggs"}, server:retrieve(ServerPid, "shopping", "eggs")),
    ServerPid.

% A "typical" session.
typical_session_test() ->
    initialize_test(),
    Session1 = spawn_link(?MODULE, typical_session_1, [self()]),
    Session2 = spawn_link(?MODULE, typical_session_2, [self()]),
    receive
        {Session1, ok} ->
            receive
                {Session2, ok} ->
                    done
            end
    end.

typical_session_1(TesterPid) ->
    {ServerPid, connected} = server:connect(server_actor),
    {ServerPid, created, "shopping"} = server:create(ServerPid, "shopping"),
    {ServerPid, stored, "shopping", "milk"} = server:store(ServerPid, "shopping", "milk", 1),
    {ServerPid, stored, "shopping", "eggs"} = server:store(ServerPid, "shopping", "eggs", 3),
    {ServerPid, retrieved, "shopping", "milk", 1} = server:retrieve(ServerPid, "shopping", "milk"),
    {ServerPid, deleted, "shopping", "eggs"} = server:delete(ServerPid, "shopping", "eggs"),
    {ServerPid, not_found, "shopping", "eggs"} = server:retrieve(ServerPid, "shopping", "eggs"),
    {ServerPid, disconnected} = server:disconnect(ServerPid),
    TesterPid ! {self(), ok}.

typical_session_2(TesterPid) ->
    {ServerPid, connected} = server:connect(server_actor),
    {ServerPid, created, "bucket"} = server:create(ServerPid, "bucket"),
    {ServerPid, stored, "bucket", "a"} = server:store(ServerPid, "bucket", "a", "1"),
    {ServerPid, retrieved, "bucket", "a", "1"} = server:retrieve(ServerPid, "bucket", "a"),
    {ServerPid, deleted, "bucket", "a"} = server:delete(ServerPid, "bucket", "a"),
    {ServerPid, not_found, "bucket", "a"} = server:retrieve(ServerPid, "bucket", "a"),
    {ServerPid, disconnected} = server:disconnect(ServerPid),
    TesterPid ! {self(), ok}.

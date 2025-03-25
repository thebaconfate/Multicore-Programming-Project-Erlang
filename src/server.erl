%% This module provides the protocol that is used to interact with a key-value
%% store.
%%
%% The interface is design to be synchronous: it waits for the reply of the
%% system.
%%
%% This module defines the public API that is supposed to be used for
%% experiments. The semantics of the API here should remain unchanged.
-module(server).

-export([connect/1, disconnect/1, create/2, store/4, retrieve/3, delete/3]).

%%
%% Server API
%%

% Connect to the server.
%
% Returns a pid that should be used for subsequent requests by this client.
-spec connect(pid()) -> {pid(), connected}.
connect(ServerPid) ->
    ServerPid ! {self(), connect},
    receive
        {ResponsePid, connected} ->
            {ResponsePid, connected}
    end.

% Disconnect from the server.
%
% This operation is not required, but may be useful depending on your
% implementation.
-spec disconnect(pid()) -> {pid(), disconnected}.
disconnect(ServerPid) ->
    ServerPid ! {self(), disconnect},
    receive
        {ResponsePid, disconnected} ->
            {ResponsePid, disconnected}
    end.

% Create a new bucket.
-spec create(pid(), string()) -> {pid(), created, string()}.
create(ServerPid, BucketName) ->
    ServerPid ! {self(), create, BucketName},
    receive
        {ResponsePid, created, BucketName} ->
            {ResponsePid, created, BucketName}
    end.

% Store a value for a key in a bucket.
%
% This does not return the value that was stored, to avoid sending a potentially
% large value back and forth.
-spec store(pid(), string(), string(), string()) -> {pid(), stored, string(), string()}.
store(ServerPid, BucketName, Key, Value) ->
    ServerPid ! {self(), store, BucketName, Key, Value},
    receive
        {ResponsePid, stored, BucketName, Key} ->
            {ResponsePid, stored, BucketName, Key}
    end.

% Retrieve a value for a key in a bucket.
-spec retrieve(pid(), string(), string()) ->
                  {pid(), retrieved, string(), string()} | {pid(), not_found, string(), string()}.
retrieve(ServerPid, BucketName, Key) ->
    ServerPid ! {self(), retrieve, BucketName, Key},
    receive
        {ResponsePid, retrieved, BucketName, Key, Value} ->
            {ResponsePid, retrieved, BucketName, Key, Value};
        {ResponsePid, not_found, BucketName, Key} ->
            {ResponsePid, not_found, BucketName, Key}
    end.

% Delete a value for a key in a bucket.
-spec delete(pid(), string(), string()) -> {pid(), deleted, string(), string()}.
delete(ServerPid, BucketName, Key) ->
    ServerPid ! {self(), delete, BucketName, Key},
    receive
        {ResponsePid, deleted, BucketName, Key} ->
            {ResponsePid, deleted, BucketName, Key}
    end.

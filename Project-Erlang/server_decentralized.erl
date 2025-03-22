-module(server_decentralized).

-export([initialize/0, initialize_with/1, server_actor/1]).

find_key(Dict, Key) ->
    dict:find(Key, Dict).

store_key(Dict, Key, Value) ->
    dict:store(Key, Value, Dict).

delete_key(Dict, Key) ->
    dict:erase(Key, Dict).

initialize() ->
    initialize_with(dict:new()).

initialize_with(Dict) ->
    ServerPid = spawn_link(?MODULE, server_actor, [Dict]),
    catch unregister(server_actor),
    register(server_actor, ServerPid),
    ServerPid.

server_actor(Dict) ->
    receive
        {Sender, connect} ->
            Sender ! {self(), connected},
            server_actor(Dict);
        {Sender, disconnect} ->
            Sender ! {self(), disconnect},
            server_actor(Dict);
        {Sender, create, Name} ->
            server_actor(Dict)
    end.

%new_bucket( Dict , Name ) ->
    %case dict:find(Name, Dict) of

        %{ok, Pid},
        %Pid ! {self(), kill},

bucket_actor(DictName, Dict, CachedKey, CachedValue) ->
    receive
        {Sender, store, Key, Value} ->
            NewDict = store_key(Dict, Key, Value),
            Sender ! {self(), stored, DictName, Key},
            bucket_actor(DictName, NewDict, CachedKey, CachedValue);
        {Sender, retrieve, Key} when CachedKey == Key ->
            Sender ! {self(), retrieved, DictName, CachedKey, CachedValue},
            bucket_actor(DictName, Dict, CachedKey, CachedValue);
        {Sender, retrieve, Key} ->
            case find_key(Dict, Key) of
                {ok, Value} ->
                    Sender ! {self(), retrieved, DictName, Key, Value},
                    bucket_actor(DictName, Dict, Key, Value);
                error ->
                    Sender ! {self(), not_found, DictName, Key},
                    bucket_actor(DictName, Dict, CachedKey, CachedValue)
            end;
        {Sender, delete, Key} when CachedKey == Key ->
            NewDict = delete_key(Dict, Key),
            Sender ! {self(), delete, DictName, Key},
            bucket_actor(DictName, NewDict);
        {Sender, delete, Key} ->
            NewDict = delete_key(Dict, Key),
            Sender ! {self(), deleted, DictName, Key},
            bucket_actor(DictName, NewDict, CachedKey, CachedValue)
    end.

bucket_actor(DictName, Dict) ->
    receive
        {Sender, store, Key, Value} ->
            NewDict = store_key(Dict, Key, Value),
            Sender ! {self(), stored, DictName, Key},
            bucket_actor(DictName, NewDict);
        {Sender, retrieve, Key} ->
            case find_key(Dict, Key) of
                {ok, Value} ->
                    Sender ! {self(), retrieved, DictName, Key, Value},
                    bucket_actor(DictName, Dict, Key, Value);
                error ->
                    Sender ! {self(), not_found, DictName, Key},
                    bucket_actor(DictName, Dict)
            end;
        {Sender, delete, Key} ->
            NewDict = delete_key(Dict, Key),
            Sender ! {self(), deleted, DictName, Key},
            bucket_actor(DictName, NewDict)
    end.

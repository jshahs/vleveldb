%%%-------------------------------------------------------------------
%% @doc vernedb top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vernedb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,get_server_pid/1,get_rr_pid/0]).

%% Supervisor callbacks
-export([init/1]).

-define(NR_OF_CHILDS, 200).
-define(TABLE, vdb_pool).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
	 [begin
         {ok, CPid} = supervisor:start_child(Pid, child_spec(I)),
	 ets:insert(?TABLE, {I,CPid})
     end || I <- lists:seq(1, ?NR_OF_CHILDS)],
%    verneDB_install:install(),
    {ok, Pid}.

get_server_pid(Key) when is_binary(Key) ->
    Id = erlang:phash2(Key, ?NR_OF_CHILDS) + 1,
    case ets:lookup(?TABLE, Id) of
        [] ->
            {error, no_bucket_found};
        [{Id, Pid}] ->
            {ok, Pid}
    end;

get_server_pid(Key)->
   case ets:lookup(?TABLE, Key) of
        [] ->
            {error, no_bucket_found};
        [{Id, Pid}] ->
            {ok, Pid}
    end.

get_rr_pid()->
	case ets:lookup(?TABLE, round_robin) of	
		[] ->
			Pid = get_server_pid(1),
			ets:insert(?TABLE,{round_robin,2}),
			Pid;
		[{_,RrNum}] ->
			Pid = get_server_pid(RrNum),
			if 
			   RrNum =:= 200 ->
				ets:insert(?TABLE,{round_robin,1});
			   true ->
				ets:insert(?TABLE,{round_robin, RrNum+1 })	
			end,
			Pid
	end.
				

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    %_ = ets:new(vernedb_rr,[public, named_table, {read_concurrency, true}]),
    _ = ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
    {ok, { {one_for_one, 5, 10}, []} }.

child_spec(I) ->
    {{vdb_pool, I},
     {vdb_user, start_link, [I]},
     permanent, 5000, worker, [vernedb_store]}.

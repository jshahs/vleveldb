%%%-------------------------------------------------------------------
%% @doc vernedb top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vernedb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(NR_OF_BUCKETS, 12).
-define(TABLE, vmq_lvldb_store_buckets).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    {ok, Pid}.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    _ = ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
    {ok, { {one_for_one, 5, 10}, []} }.

child_spec(I) ->
    {{vmq_lvldb_store_bucket, I},
     {vernedb_store, start_link, [I]},
     permanent, 5000, worker, [vernedb_store]}.


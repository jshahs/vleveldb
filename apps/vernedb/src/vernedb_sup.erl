%%%-------------------------------------------------------------------
%% @doc vernedb top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vernedb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         get_bucket_pid/1,
         get_bucket_pids/0,
         register_bucket_pid/2]).

%% Supervisor callbacks
-export([init/1]).

-define(NR_OF_BUCKETS, 12).
-define(TABLE, vmq_lvldb_store_buckets).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    [begin
         {ok, _} = supervisor:start_child(Pid, child_spec(I))
     end || I <- lists:seq(1, ?NR_OF_BUCKETS)],

    {ok, Pid}.

get_bucket_pid(Key) when is_binary(Key) ->
    Id = (erlang:phash2(Key) rem ?NR_OF_BUCKETS) + 1,
    case ets:lookup(?TABLE, Id) of
        [] ->
            {error, no_bucket_found};
        [{Id, Pid}] ->
            {ok, Pid}
    end.

get_bucket_pids() ->
    [Pid || [{_, Pid}] <- ets:match(?TABLE, '$1')].

register_bucket_pid(BucketId, BucketPid) ->
    %% Called from vmq_lvldb_store:init
    ets:insert(?TABLE, {BucketId, BucketPid}),
    ok.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    _ = ets:new(?TABLE, [public, named_table, {read_concurrency, true}]),
    {ok, { {one_for_one, 5, 10}, []} }.

child_spec(I) ->
    {{vmq_lvldb_store_bucket, I},
     {vmq_lvldb_store, start_link, [I]},
     permanent, 5000, worker, [vmq_lvldb_store]}.


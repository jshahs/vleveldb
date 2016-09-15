-module(vdb_user).
-behaviour(gen_server).

-include("../include/vdb.hrl").

%% API
-export([start_link/1,
	install_user_table/2,
	user_online/3,
	user_offline/1,
	user_status/1]).


%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {future_purpose
               }).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Id) ->
	gen_server:start_link(?MODULE, [Id], []).

install_user_table(Nodes,Frag)->
%	mnesia:stop(),
%	mnesia:create_schema(Nodes),
%	mnesia:start(),
	mnesia:create_table(vdb_users,[
                    {frag_properties,[
                        {node_pool,Nodes},{hash_module,mnesia_frag_hash},
                        {n_fragments,Frag},
                        {n_disc_copies,length(Nodes)}]
                    },
                    {index,[]},
                    {attributes,record_info(fields,vdb_users)}]).



install_subs_table(Nodes,Frag)->
%       mnesia:stop(),
%       mnesia:create_schema(Nodes),
%       mnesia:start(),
        mnesia:create_table(vdb_topics,[
                    {frag_properties,[
                        {node_pool,Nodes},{hash_module,mnesia_frag_hash},
                        {n_fragments,Frag},
                        {n_disc_copies,length(Nodes)}]
                    },
                    {index,[]},
                    {attributes,record_info(fields,vdb_topics)}]).

user_online(SubscriberId,SessionId,Node) ->
	call({online, SubscriberId, SessionId,Node}).

user_offline(SubscriberId) ->
	call( {offline, SubscriberId }).

user_uninstalled(SubscriberId) ->
        call({uninstalled, SubscriberId }).

user_status(SubscriberId) ->
        call({status, SubscriberId }).
traverse_table_and_show(Table_name)->
    Iterator =  fun(Rec,_)->
                    io:format("~p~n",[Rec]),
                    []
                end,
    case mnesia:is_transaction() of
        true -> mnesia:foldl(Iterator,[],Table_name);
        false ->
            Exec = fun({Fun,Tab}) -> mnesia:foldl(Fun, [],Tab) end,
            mnesia:activity(transaction,Exec,[{Iterator,Table_name}],mnesia_frag)
    end.


call(Req) ->
	case vernedb_sup:get_rr_pid() of
		{ok,Pid} ->
            		gen_server:call(Pid, Req, infinity);
		Res ->
			io:format("no_process~n"),
			{no_process,Res}
	end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Id]) ->
	application:start(lager),
%	application:start(plumtree),
	{ok,#state{future_purpose = Id}}.
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State) ->
    {reply, handle_req(Request, State), State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_req({online,SubscriberId,SessionId,Node},_State) ->
   Rec = #vdb_users{subscriberId = SubscriberId,status = online,on_node = Node,sessionId = SessionId},
   vdb_table_if:write(vdb_users,Rec),
   MatchSpec = [{{vdb_store,SubscriberId,'$1'},[],['$1']}],
   vdb_table_if:select(vdb_store,MatchSpec);

handle_req({offline,SubscriberId},_State) ->
   Rec = #vdb_users{subscriberId = SubscriberId,status = offline},
   vdb_table_if:write(vdb_users,Rec);

handle_req({uninstalled,SubscriberId,SessionId,Node},_State) ->
   Rec = #vdb_users{subscriberId = SubscriberId,status = uninstalled},
   vdb_table_if:write(vdb_users,Rec);

handle_req({status,SubscriberId},_State) ->
   vdb_table_if:read(vdb_users,SubscriberId);



handle_req(_,_)->
	ok.



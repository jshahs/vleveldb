-module(vdb_pub).
-behaviour(gen_server).

-include("../include/vdb.hrl").

%% API
-export([start_link/0,
	install_store_table/2,
	publish/2]).


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
start_link() ->
	gen_server:start_link({local,?MODULE},?MODULE, [], []).



install_store_table(Nodes,Frag)->
%       mnesia:stop(),
%       mnesia:create_schema(Nodes),
%       mnesia:start(),
        mnesia:create_table(vdb_store,[
                    {frag_properties,[
                        {node_pool,Nodes},{hash_module,mnesia_frag_hash},
                        {n_fragments,Frag},
                        {n_disc_copies,length(Nodes)}]
                    },
                    {index,[]},{type,bag},
                    {attributes,record_info(fields,vdb_store)}]).

publish(SubscriberId,Msg)->
	call({publish,SubscriberId,Msg}).




call(Req) ->
	%case vernedb_sup:get_rr_pid() of
	%	{ok,Pid} ->
            		gen_server:call(?MODULE, Req, infinity).
	%	Res ->
	%		io:format("no_process~n"),
	%		{no_process,Res}
	%end.

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
init(_) ->
	%application:start(lager),
%	application:start(plumtree),
	{ok,#state{future_purpose = 1}}.
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

handle_req({publish,SubscriberId,#vmq_msg{msg_ref=MsgRef,
			routing_key=RoutingKey } = Msg},_State) ->
	route_publish(RoutingKey,MsgRef,Msg);


handle_req(_,_)->
	ok.



route_publish(RoutingKey,MsgRef,Msg) ->
	case vdb_table_if:read(vdb_topics,[{RoutingKey,1}]) of
		[] ->
			[];
		Recs when is_list(Recs) ->
			session(Recs,MsgRef,Msg);
		Rec ->
		   case vdb_table_if:read(vdb_users,Rec#vdb_topics.subscriberId) of
			[] ->
				[];
			#vdb_users{status = online} = Usr  ->
				[Usr#vdb_users.on_node,Usr#vdb_users.sessionId];
			#vdb_users{status = offline} = Usr  ->
				spawn(?MODULE,write_store,[Rec#vdb_topics.subscriberId,Msg]),
				[]
		  end
	end.

session(Recs,MsgRef,Msg) ->
	UserTab = [vdb_table_if:read(vdb_users,X#vdb_topics.subscriberId) ||X <- 
					vdb_table_if:read(vdb_topics,[{[<<"junnu">>],1}])],
	InactiveUsers = [X#vdb_users.subscriberId || X <- UserTab,X#vdb_users.status == offline],
	spawn(?MODULE,handle_offline_msgs,[InactiveUsers,MsgRef,Msg]),
	ActiveUsers = [{X#vdb_users.on_node,X#vdb_users.sessionId} || X <- UserTab, X#vdb_users.status == online ], 
	Nodes = lists:usort([X#vdb_users.on_node || X <- UserTab, X#vdb_users.status == online]),
	[session_info(X,ActiveUsers) || X <- Nodes,Nodes =/= []].

session_info(_Node,[])->
	{};

session_info([],_)->
	{};

session_info(Node,ActiveUsers) ->
	SessionInfo = [X || {X,Y} <- ActiveUsers,X=:= Node],
	{Node,SessionInfo}.

handle_offline_msgs([],_,_) ->
	ok;
handle_offline_msgs(InactiveUsers,MsgRef,Msg) ->
	[write_store(X,Msg) || X <- InactiveUsers],
	ok.

write_store(SubId,Msg)->
	Rec = #vdb_store{subscriberId = SubId,vmq_msg = Msg},
	vdb_table_if:write(vdb_store,Rec).	

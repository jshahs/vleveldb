-module(vernedb_store).
-include("vmq_server.hrl").
-behaviour(gen_server).

%% API
-export([start_link/1,
	msg_store_read_plum/2,
	msg_store_delete_plum/2,
	msg_store_write_plum/2]).


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
    gen_server:start_link(?MODULE, [], []).

msg_store_write_plum(SubscriberId, #vmq_msg{msg_ref=MsgRef} = Msg) ->
    call(MsgRef, {write_plum, SubscriberId, Msg}).

msg_store_delete_plum(SubscriberId, #vmq_msg{msg_ref=MsgRef} = Msg) ->
    call(MsgRef, {delete_plum, SubscriberId, Msg}).

msg_store_read_plum(SubscriberId, MsgRef) ->
    call(MsgRef, {read_plum, SubscriberId, MsgRef}).


call(Key, Req) ->
            gen_server:call(?MODULE, Req, infinity).

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
	#state{future_purpose = ok}.
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

handle_req({write_plum, {MP, _} = SubscriberId,
            #vmq_msg{msg_ref=MsgRef, mountpoint=MP, dup=Dup, qos=QoS,
                     routing_key=RoutingKey, payload=Payload} = VmqMsg},
           _State) ->
   MsgKey = sext:encode({msg, MsgRef, SubscriberId}),
   Val = term_to_binary(VmqMsg),
   plumtree_metadata:put({oktalk,offline_store},MsgKey,Val);


handle_req({read_plum, {MP, _} = SubscriberId, MsgRef},
           _State) ->
   MsgKey = sext:encode({msg, MsgRef, SubscriberId}),
   Val = plumtree_metadata:get({oktalk,offline_store},MsgKey,[{default, []}]),
   VmqMsg = binary_to_term(Val),
   {ok,VmqMsg};

handle_req({delete_plum, {MP, _} = SubscriberId, MsgRef},
           _State) ->
   MsgKey = sext:encode({msg, MsgRef, SubscriberId}),
   plumtree_metadata:delete({oktalk,offline_store},MsgKey,[{default, []}]),
   ok.

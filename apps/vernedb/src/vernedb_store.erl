-module(vernedb_store).
-include("vmq_server.hrl").
-behaviour(gen_server).

-include("../include/verneDB.hrl").

%% API
-export([start_link/1,
	get_offline_msg_number/1,
	get_msg/2,
	install_table/3,
	mnesia_write/2,
	rpc_write/2,
	mnesia_read/1,
	total_offline_msgs/0,
	total_subscriptions/0,
	msg_store_read_plum/1,
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
	gen_server:start_link(?MODULE, [Id], []).

install_table(Nodes,Table,Frag)->
	mnesia:stop(),
	mnesia:create_schema(Nodes),
	mnesia:start(),
	mnesia:create_table(Table,[
                    {frag_properties,[
                        {node_pool,Nodes},{hash_module,mnesia_frag_hash},
                        {n_fragments,Frag},
                        {n_disc_copies,length(Nodes)}]
                    },
                    {index,[]},{type, bag},
                    {attributes,record_info(fields,vmq_offline_store)}]).

msg_store_write_plum({[],ClientId} = SubscriberId, #vmq_msg{msg_ref=MsgRef} = Msg) ->
    call(ClientId, {write_plum, SubscriberId, Msg}).

msg_store_delete_plum(SubscriberId, #vmq_msg{msg_ref=MsgRef} = Msg) ->
    call(MsgRef, {delete_plum, SubscriberId, Msg}).

msg_store_read_plum({[],ClientId} = SubscriberId) ->
    call(ClientId,{read_plum, SubscriberId}).

rpc_write(SubscriberId,VmqMsg)->
   Rec = #vmq_offline_store{subscriberId = SubscriberId,vmq_msg = VmqMsg},
   Write = fun(Rec) ->
             case mnesia:dirty_write(vmq_offline_store,Rec) of
                ok ->
                        {ok,updated};
                Res ->
                        {error,Res}
             end
        end,
   mnesia:activity(sync_dirty, Write,[Rec],mnesia_frag).

mnesia_write({[],ClientId} = SubscriberId, #vmq_msg{msg_ref=MsgRef} = Msg) ->
	call(ClientId, {write_mnesia, SubscriberId, Msg}).

mnesia_read({[],ClientId} = SubscriberId) ->
    call(ClientId,{read_mnesia, SubscriberId}).

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

total_offline_msgs() ->
        traverse_table_and_show(vmq_offline_store).

total_subscriptions() ->
    Total = plumtree_metadata:fold(
              fun ({_, '$deleted'}, Acc) -> Acc;
                  ({_, Subs}, Acc) ->
                      Acc + length(Subs)
              end, 0, {oktalk,msgref},
              [{resolver, lww}]),
    [{total, Total}].

call(Key, Req) ->
	%case vernedb_sup:get_rr_pid() of
	case vernedb_sup:get_server_pid(Key) of
		{ok,Pid} ->
            		gen_server:call(Pid, Req, infinity);
		Res ->
			io:format("no_process~n"),
			{no_process,Res,Key}
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

update_offline_ref(SubscriberId,MsgRef) ->
	case plumtree_metadata:get({oktalk,msgref}, SubscriberId) of
                undefined ->
		   plumtree_metadata:put({oktalk,msgref}, SubscriberId,
                                          [MsgRef]);
                [] ->
		   plumtree_metadata:put({oktalk,msgref}, SubscriberId,
                                          [MsgRef]);
                Refs ->
		    NewRef = [MsgRef|Refs],
                    plumtree_metadata:put({oktalk,msgref}, SubscriberId,
                                          NewRef)
            end.

get_offline_ref(SubscriberId)->
   case plumtree_metadata:get({oktalk,msgref}, SubscriberId) of
                undefined ->
			not_found;
                [] ->
			not_found;
                Refs ->
		  Refs
            end.

get_offline_msg_number(SubscriberId)->
   case plumtree_metadata:get({oktalk,msgref}, SubscriberId) of
                undefined ->
                        not_found;
                [] ->
                        not_found;
                Refs ->
                  length(Refs)
            end.


get_msg(SubscriberId,MsgRef)->
   MsgKey = sext:encode({msg, MsgRef, SubscriberId}),
   case plumtree_metadata:get({oktalk,offline_store},MsgKey,[{default, []}]) of
        [] ->
                not_found;
        Val ->
                VmqMsg = binary_to_term(Val),
                VmqMsg
  end.

handle_req({read_plum, {MP, _} = SubscriberId},
           _State) ->
   MsgRefs = get_offline_ref(SubscriberId),
   case MsgRefs of
        not_found ->
                not_found;
        Refs ->
		io:format("The ref::~p~n",[Refs]),
                AllMsgs = [get_msg(SubscriberId,X)|| X <- Refs],
		delete_msgs(SubscriberId),
		AllMsgs
   end;

handle_req({write_plum, {MP, _} = SubscriberId,
            #vmq_msg{msg_ref=MsgRef, mountpoint=MP, dup=Dup, qos=QoS,
                     routing_key=RoutingKey, payload=Payload} = VmqMsg},
           _State) ->
   Res = update_offline_ref(SubscriberId,MsgRef),
   MsgKey = sext:encode({msg, MsgRef, SubscriberId}),
   Val = term_to_binary(VmqMsg),
   plumtree_metadata:put({oktalk,offline_store},MsgKey,Val);

handle_req({write_mnesia,{MP, _} = SubscriberId,
	#vmq_msg{msg_ref=MsgRef, mountpoint=MP, dup=Dup, qos=QoS,
                     routing_key=RoutingKey, payload=Payload} = VmqMsg},	
	_State) ->
   Rec = #vmq_offline_store{subscriberId = SubscriberId,vmq_msg = VmqMsg},
   Write = fun(Rec) -> 
   	     case mnesia:write(Rec) of
                ok ->
                        {ok,updated};
                Res ->
                        {error,Res}
             end
	end,
   mnesia:activity(sync_dirty, Write,[Rec],mnesia_frag);

handle_req({read_mnesia,{MP, _} = SubscriberId},_State) ->
	Read = fun(SubscriberId) ->
   		case mnesia:dirty_read({vmq_offline_store,SubscriberId}) of
		  [ValList] ->
			ValList;
		   Res ->
			Res
		end
	end,
	mnesia:activity(transaction, Read, [SubscriberId], mnesia_frag);
	


handle_req(_,_)->
	ok.

delete_msgs(SubscriberId)->
	[delete_msg(SubscriberId,X) || X<- get_offline_ref(SubscriberId)],
	plumtree_metadata:delete({oktalk,msgref},SubscriberId,[{default, []}]).

delete_msg(SubscriberId,MsgRef)->
   MsgKey = sext:encode({msg, MsgRef, SubscriberId}),
   plumtree_metadata:delete({oktalk,offline_store},MsgKey,[{default, []}]),
   ok.

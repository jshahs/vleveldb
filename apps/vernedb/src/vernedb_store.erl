-module(vernedb_store).
-include("vmq_server.hrl").
-behaviour(gen_server).

-include("../include/vernedb.hrl").

%% API
-export([start_link/1,
	install_subscription/2,
	install_table/2,
	mnesia_write/2,
	mnesia_read/1,
	total_offline_msgs/0,
	total_subscriptions/0]).


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

install_table(Nodes,Frag)->
%	mnesia:stop(),
%	mnesia:create_schema(Nodes),
%	mnesia:start(),
	mnesia:create_table(vmq_offline_store,[
                    {frag_properties,[
                        {node_pool,Nodes},{hash_module,mnesia_frag_hash},
                        {n_fragments,Frag},
                        {n_disc_copies,length(Nodes)}]
                    },
                    {index,[]},{type, bag},
                    {attributes,record_info(fields,vmq_offline_store)}]).


install_subscription(Nodes,Frag)->
mnesia:create_table(vmq_subscriber,[
                    {frag_properties,[
                        {node_pool,Nodes},{hash_module,mnesia_frag_hash},
                        {n_fragments,Frag},
                        {n_disc_copies,length(Nodes)}]
                    },
                    {index,[]},{type, bag},
                    {attributes,record_info(fields,vmq_subscriber)}]).


add_subscriber({[],ClientId} = SubscriberId,Topic) ->
	call(ClientId, {add_subscriber, SubscriberId, Topic }).

read_subsciptions({[],ClientId} = SubscriberId) ->
	call(ClientId, {read_subs, SubscriberId}).

all_subscriptions()->
	call("all",all_subs).

delete_subscriber({[],ClientId} = SubscriberId,Topic) ->
	call(ClientId, {delete_subscriber, SubscriberId, Topic }).

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
	case vernedb_sup:get_rr_pid() of
%	case vernedb_sup:get_server_pid(Key) of
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

handle_req({read_subscriber,{MP, _} = SubscriberId},_State) ->
        Read = fun(SubscriberId) ->
                case mnesia:read({vmq_subscriber,SubscriberId}) of
                  [ValList] ->
                        ValList;
                   Res ->
                        Res
                end
        end,
        ValList = mnesia:activity(sync_dirty, Read, [SubscriberId], mnesia_frag),
        ValList;

handle_req({add_subscriber,{MP, _} = SubscriberId,Topic},
        _State) ->
   Rec = #vmq_subscriber{subscriberId = SubscriberId,topic = Topic},
   Write = fun(Rec) ->
             case mnesia:write(Rec) of
                ok ->
                        {ok,updated};
                Res ->
                        {error,Res}
             end
        end,
   mnesia:activity(sync_dirty, Write,[Rec],mnesia_frag);


handle_req(all_subs,_State)->
	traverse_table_and_show(vmq_subscriber);

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
   		case mnesia:read({vmq_offline_store,SubscriberId}) of
		  [ValList] ->
			ValList;
		   Res ->
			Res
		end
	end,
	ValList = mnesia:activity(sync_dirty, Read, [SubscriberId], mnesia_frag),
	ok = delete_offline(SubscriberId),
	ValList;


handle_req(_,_)->
	ok.

delete_offline(Key)->
	Del = fun(Key) -> mnesia:delete({vmq_offline_store,Key}) end,
	mnesia:activity(sync_dirty, Del, [Key], mnesia_frag).


-module(vdb_table_if).

-export([write/2,read/2,delete/2]).



write(Tab,Rec)->
   Write = fun(Rec) ->
             case mnesia:write(Rec) of
                ok ->
                        {ok,updated};
                Res ->
                        {error,Res}
             end
   end,
   mnesia:activity(sync_dirty, Write,[Rec],mnesia_frag).

read(Tab,Key) ->
	Read = fun(Tab,Key) ->
                case mnesia:read({Tab,Key}) of
                  [ValList] ->
                        ValList;
                   Res ->
                        Res
                end
        end,
        ValList = mnesia:activity(sync_dirty, Read, [Tab,Key], mnesia_frag),
	ValList.


delete(Tab,Key)->
        Del = fun(Tab,Key) -> mnesia:delete({Tab,Key}) end,
        mnesia:activity(sync_dirty, Del, [Tab,Key], mnesia_frag).
	

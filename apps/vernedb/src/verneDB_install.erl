-module(verneDB_install).

-include("../include/verneDB.hrl").

-export([
         install/0
        ]).

-define(TABLES,
        [{vmq_offline_store,record_info(fields, vmq_offline_store)}]).

install() ->
    mnesia:create_schema([node()]),
    create_tables().

create_tables() ->
    Nodes = [{ram_copies,       mnesia:table_info(schema, ram_copies)},
             {disc_copies,      mnesia:table_info(schema, disc_copies)},
             {disc_only_copies, mnesia:table_info(schema, disc_only_copies)}],
    create_tables(?TABLES, Nodes).

create_tables([{vmq_trie_subs, Attrs, Options} | Tables], Nodes) ->
    NumCopies = case length([node() | nodes()]) of
                    1 ->
                        1;
		    2 ->
			1;
                    4 ->
                        2
                end,
    Res = mnesia:create_table(vmq_trie_subs, [{attributes, Attrs},{type, bag},
                                  {frag_properties, [{hash_module,  verneDB_frag_hash},
                                                     {n_fragments, 128},
                                                     {n_ram_copies, NumCopies},
						     {node_pool, mnesia:table_info(schema, disc_copies)}]} |
                                  Options]),
    io:format("Result of creating vmq_trie_subs --> ~1000.p~n", [Res]),
    create_tables(Tables, Nodes);
create_tables([{Tab, Attrs} | Tables], Nodes) ->
    create_tables([{Tab, Attrs, []} | Tables], Nodes);
create_tables([{Tab, Attrs, Options} | Tables], Nodes) ->
    Res = mnesia:create_table(Tab, [{attributes, Attrs} | Nodes] ++ Options),
    io:format("Result of creating ~p --> ~1000.p~n", [Tab, Res]),
    create_tables(Tables, Nodes);
create_tables([], _) ->
    io:format("Done~n", []).

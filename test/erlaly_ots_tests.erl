%%%-------------------------------------------------------------------
%%% @author Kong Fanbin <kfbuniversity@gmail.com>
%%% @copyright (C) 2012, Kong Fanbin
%%% @doc
%%%
%%% @end
%%% Created : 19 Feb 2012 by Kong Fanbin <kfbuniversity@gmail.com>
%%%-------------------------------------------------------------------
-module(erlaly_ots_tests).

-include_lib("eunit/include/eunit.hrl").
-include("erlaly_ots.hrl").

-import(erlaly_util, [guess_type/1]).
-import(demo_util, [generate_col_rcds/2, generate_data_op/2,
		    get_row_data_op/1, get_row_data_op/2,
		    get_row_by_offset_data_op/2, get_row_by_offset_data_op/3,
		    get_row_by_range_data_op/1, get_row_by_range_data_op/2]).

%%-export([table_group_op_test_not/0, table_op_test_not/0]).
-define(setup(F), {setup, fun start/0, fun stop/1, F}).

-define(data_setup(F), {setup, fun data_start/0, fun data_stop/1, F}).

%% Careful!
%%   The test case may damage your ots table.
%%   Make sure the table name defined here is not used in your
%%   OTS currently. 
-define(TestTableName, "TestTable").

%% Careful!
%%   The test case may damage your ots table group.
%%   Make sure the table group name defined here is not used in your
%%   OTS. 
-define(TestTableGroupName, "TestTableGroup").
%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%
start() ->
    erlaly:start(),
    erlaly_ots:init("wnh91cxrd3xw7hgxnrlsu131", "d21NPKrin2QXLGowsuFcPtlmumI=").

data_start() ->
    erlaly:start(),
    erlaly_ots:init("wnh91cxrd3xw7hgxnrlsu131", "d21NPKrin2QXLGowsuFcPtlmumI="),
    TableMetaW = table_meta(),
    erlaly_ots:create_table(TableMetaW).

stop(_) ->
    ok.

data_stop(_) ->
    erlaly_ots:delete_table(?TestTableName),
    ok.

%%-define(TEST_All, yes).

-ifdef(TEST_All).

-define(TEST_TABEL_GROUP, yes).
-define(TEST_TABEL, yes).
-define(TEST_DATA_OPS, yes).
-define(TEST_TRANSACTION, yes).

-else.

%%-define(TEST_TABEL_GROUP, yes).
%%-define(TEST_TABEL, yes).
%%-define(TEST_DATA_OPS, yes).
-define(TEST_TRANSACTION, yes).

-endif.

%%---------------------------------------------------------------------------------------------%%

-ifdef(TEST_TABEL_GROUP).

table_group_op_test_() ->
    [
     {"---- Test table group operations.",
      ?setup(fun(_) -> 
		     [
		      list_table_group(),
		      create_table_group(),
		      delete_table_group(),
		      table_group_ops()]
	     end)}
    ].

list_table_group() ->
     ?_assertMatch({ok, _TableGroups, _, _}, erlaly_ots:list_table_group()).

create_table_group() ->
     ?_assertMatch({ok, _, _, _}, erlaly_ots:create_table_group(?TestTableGroupName, string)).

delete_table_group() ->
     ?_assertMatch({ok, _Code1, _, _}, erlaly_ots:delete_table_group(?TestTableGroupName)).

table_group_ops() ->
    {ok, OrigTableGroups, _, _} = erlaly_ots:list_table_group(),
%%    ?debugFmt("~n-------- OrigTableGroups --------~n~p~n", [OrigTableGroups]),
    {ok, _, _, _} = erlaly_ots:create_table_group(?TestTableGroupName, string),
    {ok, _, _, _} = erlaly_ots:delete_table_group(?TestTableGroupName),
     ?_assertMatch({ok, OrigTableGroups, _, _}, erlaly_ots:list_table_group()).

-endif.

%%---------------------------------------------------------------------------------------------%%

-ifdef(TEST_TABEL).
table_op_test_() ->
    [
     {"---- Test table operations",
      ?setup(fun(_) -> 
		     [
		      list_table(),
		      create_table(),
		      get_table_meta(),
		      delete_table(),
		      table_ops()
		     ]
	     end)},

     {"---- Tests for bad table operations",
      ?setup(fun bad_table_ops/1)}
    ].

list_table() ->
     ?_assertMatch({ok, _Tables, _, _}, erlaly_ots:list_table()).

create_table() ->
    TableMetaW = table_meta(),
    ?_assertMatch({ok, _, _, _}, erlaly_ots:create_table(TableMetaW)).

get_table_meta() ->
    TableMetaW = table_meta(),
    ?_assertMatch({ok, TableMetaW, _, _},  erlaly_ots:get_table_meta(?TestTableName)).

delete_table() ->
    ?_assertMatch({ok, _, _, _}, erlaly_ots:delete_table(?TestTableName)).
    
table_ops() ->
    TableMetaW = table_meta(),
    {ok, OrigTables, _, _} = erlaly_ots:list_table(),
%%    ?debugFmt("~n-------- OrigTables  --------~n~p~n", [OrigTables]),
    {ok, _, _, _} = erlaly_ots:create_table(TableMetaW),
    {ok, _TablesAfterCreate, _, _} = erlaly_ots:list_table(),
    {ok, TableMetaR, _, _} = erlaly_ots:get_table_meta(?TestTableName),
    {ok, _Code, _, _} = erlaly_ots:delete_table(?TestTableName),
    [
     ?_assertMatch(TableMetaW, TableMetaR),
     ?_assertMatch({ok, OrigTables, _, _}, erlaly_ots:list_table())
    ].

bad_table_ops(_NotUsed) ->
    TableMetaW = table_meta(),
    [
     ?_assertMatch({ok, _, _, _}, erlaly_ots:create_table(TableMetaW)),
     ?_assertMatch({error, {"OTSStorageObjectAlreadyExist", _, _, _}}, erlaly_ots:create_table(TableMetaW)),
     ?_assertMatch({ok, _Code, _, _}, erlaly_ots:delete_table(?TestTableName)),
     ?_assertMatch({error, {"OTSStorageObjectNotExist", _, _, _}}, erlaly_ots:delete_table(?TestTableName)),
     ?_assertError(function_clause, erlaly_ots:create_table(bad_pk_table_meta())),
     ?_assertError({case_clause, {paging_key_len, bad_pkl}}, erlaly_ots:create_table(bad_view_table_meta()))
    ].

-endif.

%%---------------------------------------------------------------------------------------------%%

-ifdef(TEST_TRANSACTION).
transaction_test_() ->
    [{"---- Tests for start transaction.",
      ?data_setup(fun test_start_transaction/1)},
     {"---- Tests for start transaction again.",
      ?data_setup(fun test_start_transaction_again/1)},
     {"---- Tests for commit transaction.",
      ?data_setup(fun test_commit_transaction/1)},
     {"---- Tests for abort transaction.",
      ?data_setup(fun test_abort_transaction/1)}
    ].
    
test_start_transaction(_NotUsed) ->
    {timeout, 120, ?_assertMatch({ok, _, _, _}, erlaly_ots:start_transaction(?TestTableName, "U00001"))}.

test_start_transaction_again(_NotUsed) ->
    %% 第一次事务在未进行 commit 或 abort 之前，再发一次 start_transaction 会产生这样的错误：
    {timeout, 120, ?_assertMatch({error, {"OTSStorageTxnLockKeyFail", _, _, _}}, erlaly_ots:start_transaction(?TestTableName, "U00001"))}.

test_commit_transaction(_NotUsed) ->
    {ok, TransactionID1, _, _} = erlaly_ots:start_transaction(?TestTableName, "U00001"),
%%    ?debugFmt("TransactionID1=~p~n", [TransactionID1]),
    [
     {timeout, 120, ?_assertMatch({ok, _, _, _}, erlaly_ots:commit_transaction(TransactionID1))}
    ].

test_abort_transaction(_NotUsed) ->
    {ok, TransactionID2, _, _} = erlaly_ots:start_transaction(?TestTableName, "U00001"),
%%    ?debugFmt("TransactionID2=~p~n", [TransactionID2]),
    [
     {timeout, 120, ?_assertMatch({ok, _, _, _}, erlaly_ots:abort_transaction(TransactionID2))}
    ].
    
-endif.

%%---------------------------------------------------------------------------------------------%%

-ifdef(TEST_DATA_OPS).

data_op_test_() ->
     [{"---- Tests for data operations.",
       ?setup(fun data_ops/1)}
     ].

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%
data_ops(_NotUsed) ->
    %% Make sure the test table exists
    %% 确保测试的表存在
    TableMetaW = table_meta(),
    erlaly_ots:create_table(TableMetaW),
%%    {ok, _, _, _} = erlaly_ots:create_table(TableMetaW),
    MultiRowColRcds = multi_row_col_rcds(),
    %% 把每一行 column 记录映射成一个 put 类型的 data_op 记录
    PutDataOps = [generate_data_op(RowColRcds, put) || RowColRcds <- MultiRowColRcds],
    io:format("PutDataOps:~p~n", [PutDataOps]),

    %% 获取每行所有列的数据
    %% TODO: 增加获取指定列的测试
    GetRowDataOps = [get_row_data_op(RDO) || RDO <- MultiRowColRcds],

    %% 把每一行 column 记录映射成一个 delete 类型的 data_op 记录
    DeleteDataOps = [generate_data_op(RowColRcds, delete) || RowColRcds <- MultiRowColRcds],
    [
     ?_assertMatch({ok, _, _, _}, erlaly_ots:create_table(TableMetaW)),
     %% 测试 put_data 接口
     test_put_data(PutDataOps),

     %% 测试 get_row 接口
     test_get_row(GetRowDataOps),

     %% 测试 put_data 写入的数据和 get_row 读出的数据一致
     put_get(MultiRowColRcds),
     
     %% 测试 delete_data 接口
     test_delete_data(DeleteDataOps),
     
     %% 测试 batch_modify_data 接口
     test_batch_modify_data(PutDataOps),
     
     batch_put_get(),
     ?_assertMatch({ok, _Code, _, _}, erlaly_ots:delete_table(?TestTableName)),
     ?_assertEqual(1,1)
    ].

-endif.

%% test_batch_modify_data 和 batch_put_get 同时测试时
%% transaction 会有问题
test_batch_modify_data(TestRowDataOps) ->
    %% 按数据分片键的值将列表拆分
    %% 相同的数据分片键将他们放在同一个批处理请求中
    BatchDataOps = lists:foldl(
		     fun(DataOp, AccIn) ->
			     #data_op{primary_keys = PrimaryKeys} = DataOp,
			     PartitionKeyValue = (hd(PrimaryKeys))#primary_key.value,
			     %% 取出数据分片键为 PartitionKeyValue 的 data_op 记录
			     case lists:keytake(PartitionKeyValue, 1, AccIn) of
				 false ->
				     %% 如果没有则创建一个 tuple 来保存这样的一个对
				     [{PartitionKeyValue, [DataOp]} | AccIn];
				 {value, {_, DataOps}, Rest} ->
				     %% 如果有则追加上去
				     [{PartitionKeyValue, [DataOp | DataOps]} | Rest]
			     end
		     end, [], TestRowDataOps),
    %% 保证同一数据分片键内的 data_op 记录的顺序与原来一致
    %% 如果要求不同数据分片键的 data_op 记录顺序与原来一致，那么此函数不适用
    lists:map(
      fun({PartitionKeyValue, DataOps}) ->
	      test_batch_modify_data(PartitionKeyValue, lists:reverse(DataOps))
      end, BatchDataOps).

test_batch_modify_data(PartitionKeyValue, DataOps) ->
    {ok, TransactionID, _, _} = erlaly_ots:start_transaction(?TestTableName, PartitionKeyValue),
    Result = erlaly_ots:batch_modify_data(?TestTableName, DataOps, TransactionID),
    {ok, _, _, _} = erlaly_ots:commit_transaction(TransactionID),
%%    ?debugFmt("commit_transaction ok~n", []),
    ?_assertMatch({ok, _, _, _}, Result).
    
batch_put_get() ->
    %% 要求 MultiRowColRcds 中的每一行数据分片键都等于 PartitionKeyValue
    {PartitionKeyValue, MultiRowColRcds} = same_key_row_col_rcds(),

    %% 把每一行 column 记录映射成一个 put 类型的 data_op 记录
    PutDataOps = [generate_data_op(RowColRcds, put) || RowColRcds <- MultiRowColRcds],

    {ok, TransactionID, _, _} = erlaly_ots:start_transaction(?TestTableName, PartitionKeyValue),
    %% 批量写入数据
%%    ?debugFmt("batch PutDataOps:~n~p~n", [PutDataOps]),
    {ok, _, _, _} = erlaly_ots:batch_modify_data(?TestTableName, PutDataOps, TransactionID),

    {ok, _, _, _} = erlaly_ots:commit_transaction(TransactionID),

    GetRowAllDataOps = get_row_by_range_data_op(hd(MultiRowColRcds)),
%%    ?debugFmt("GetRowAllDataOps:~n~p~n", [GetRowAllDataOps]),
    %% 通过 get_rows_by_range 读出刚刚写入的数据
    {ok, Results, _, _} = erlaly_ots:get_rows_by_range(?TestTableName, GetRowAllDataOps),

    %% 从分页键的第0行偏移开始读出100行
    GetRowOffsetDataOps = get_row_by_offset_data_op(paging_col_rcds(), {0, 100}),
%%    ?debugFmt("GetRowOffsetDataOps:~n~p~n", [GetRowOffsetDataOps]),
    %% 通过 get_rows_by_offset 读出刚刚写入的数据
    {ok, Results2, _, _} = erlaly_ots:get_rows_by_offset(?TestTableName, GetRowOffsetDataOps),
    [
     ?_assertEqual(sort_row_and_cols(Results), sort_row_and_cols(MultiRowColRcds)),
     ?_assertEqual(sort_row_and_cols(Results2), sort_row_and_cols(MultiRowColRcds))
     ].

test_get_row(GetRowDataOps) ->
    lists:map(fun(X) ->
		      ?_assertMatch({ok, _, _, _}, erlaly_ots:get_row(?TestTableName, X))
	      end, GetRowDataOps).

test_put_data(PutDataOps) ->
    lists:map(fun(X) ->
		      ?_assertMatch({ok, _, _, _}, erlaly_ots:put_data(?TestTableName, X))
	      end, PutDataOps).

put_get(MultiRowColRcds) ->
    Results = lists:map(
		fun(X) ->
			%% 写入一行数据
			PutDataOp = generate_data_op(X, put),    
			{ok, _, _, _} = erlaly_ots:put_data(?TestTableName, PutDataOp),
			%% 读出刚刚写入的数据
			GetRowDataOp = get_row_data_op(X),
			{ok, RowResult, _, _} = erlaly_ots:get_row(?TestTableName, GetRowDataOp),
			lists:flatten(RowResult)
		end, MultiRowColRcds),
    ?_assertEqual(sort_row_cols(Results), sort_row_cols(MultiRowColRcds)).

test_delete_data(DeleteDataOps) ->
    lists:map(fun(X) ->
		      ?_assertMatch({ok, _, _, _}, erlaly_ots:delete_data(?TestTableName, X))
	      end, DeleteDataOps).


%% 将每一行中的列进行排序，行的顺序将保持不变
%% 注意：不是对行进行排序
sort_row_cols(MultiRowColRcds) ->
    lists:map(
      %% X 是行数据
      fun(X) ->
	      lists:sort(
		%% A, B 为 X 行中的两列
		fun(#column{name = NameA}, #column{name = NameB}) ->
			NameA > NameB
		end, X)
      end, MultiRowColRcds).
	      
%% 将列和行都进行排序
sort_row_and_cols(MultiRowColRcds) ->
    lists:sort(
      %% RowA, RowB 是行数据
      fun(RowA, RowB) ->
	      %% 仅以该行第三列(即 FromAddr)进行排序
	      %% 此方法仅适用本测试数据，正常情况下，要想保证排序唯一性
	      %% 还应比较其他列
	      #column{value = FromAddrA} = lists:nth(3, RowA),
	      #column{value = FromAddrB} = lists:nth(3, RowB),
	      FromAddrA > FromAddrB
      end, sort_row_cols(MultiRowColRcds)).

paging_col_rcds() ->	      
    ColAttrs = [{true, "UserID"}, {true, "ReceiveTime"}],
    RowData = ["U0001", "2012-2-24"],
    %% 把 paging 列的数据和 paging 列的属性映射成行为一个 column 记录
    generate_col_rcds(ColAttrs, RowData).
    
same_key_row_col_rcds() ->
    ColAttrs = [{true, "UserID"}, {true, "ReceiveTime"}, {true, "FromAddr"},
		{false, "ToAddr"}, {false, "MailSize"}, {false, "Subject"}, {false, "Read"}],
    MultiRowsData = [["U0001", "2012-2-24", "eric@demo.com", "bob@demo.com", 1000, "Hello", true],
		     ["U0001", "2012-2-24", "alice@demo.com", "bob@demo.com;vivian@demo.com", 15000, "Fw:greeting", true],
		     ["U0001", "2012-2-24", "bob@demo.com", "team1@demo.com", 8900, "Review", false],
		     ["U0001", "2012-2-24", "windy@demo.com", "tom@demo.com", 500, "Meeting Request", true]
		    ],
    %% 把多行数据和列属性映射成多行的column记录
    {"U0001", lists:map(fun(RowData) ->
				generate_col_rcds(ColAttrs, RowData)
			end, MultiRowsData)}.

multi_row_col_rcds() ->
    ColAttrs = [{true, "UserID"}, {true, "ReceiveTime"}, {true, "FromAddr"},
		{false, "ToAddr"}, {false, "MailSize"}, {false, "Subject"}, {false, "Read"}],
    MultiRowsData = [["U0001", "1998-1-1", "eric@demo.com", "bob@demo.com", 1000, "Hello", true],
		     ["U0001", "2011-10-20", "alice@demo.com", "bob@demo.com;vivian@demo.com", 15000, "Fw:greeting", true],
		     ["U0001", "2011-10-21", "alice@demo.com", "team1@demo.com", 8900, "Review", false],
		     ["U0002", "2010-3-18", "windy@demo.com", "tom@demo.com", 500, "Meeting Request", true]
		    ],
    %% 把多行数据和列属性映射成多行的column记录
    lists:map(fun(RowData) ->
		      generate_col_rcds(ColAttrs, RowData)
	      end, MultiRowsData).

table_meta() ->
    %% Table Primary Keys
    %% 表的主键列，其中第一列：UserID为数据分片键
    TablePKs = [#primary_key{name = "UserID",
			     type = string},
		#primary_key{name = "ReceiveTime",
			     type = string},
		#primary_key{name = "FromAddr",
			     type = string}],
    
    %% View Primary Keys
    %% 视图的主键列，其中第一列：UserID为数据分片键
    %% 视图主键列包含原表的所有主键列，以保证唯一性
    %% 数据分片键必须和原表一致，其他列的顺序个数没有限制
    ViewPKs = [#primary_key{name = "UserID",
			    type = string},
	       #primary_key{name = "FromAddr",
			    type = string},
	       #primary_key{name = "ReceiveTime",
			    type = string},
	       #primary_key{name = "MailSize",
			    type = integer}
	      ],
    
    Columns = [#column{name = "ToAddr",
		       type = string},
	       #column{name = "MailSize",
		       type = integer}
	       #column{name = "Subject",
		       type = string},
	       #column{name = "Read",
		       type = boolean}],

    Views = [#view{name = "view1",
		   primary_keys = ViewPKs,
		   %% 此视图将前两列 UserID 和 FromAddr 作为分页键
		   paging_key_len = 2,
		   columns = Columns}],
    
    Table1 = #table{name = ?TestTableName,
		   primary_keys = TablePKs,
		   %% 原表将前两列 UserID 和 ReceiveTime 作为分页键
		   paging_key_len = 2,
		   views = Views
		  },

%%    ?debugFmt("~n-------- table to create --------~n~p~n", [Table1]),
    Table1.
    
not_matched_table_meta() ->
    %% Table Primary Keys
    %% 表的主键列，其中第一列：UserID为数据分片键
    TablePKs = [#primary_key{name = "UserID",
			     type = string},
		#primary_key{name = "ReceiveTime",
			     type = string},
		#primary_key{name = "FromAddr",
			     type = integer}],
    
    %% View Primary Keys
    %% 视图的主键列，其中第一列：UserID为数据分片键
    %% 视图主键列包含原表的所有主键列，以保证唯一性
    %% 数据分片键必须和原表一致，其他列的顺序个数没有限制
    ViewPKs = [#primary_key{name = "UserID",
			    type = string},
	       #primary_key{name = "FromAddr",
			    type = integer},
	       #primary_key{name = "ReceiveTime",
			    type = string},
	       #primary_key{name = "MailSize",
			    type = integer}
	      ],
    
    Columns = [#column{name = "ToAddr",
		       type = string},
	       #column{name = "MailSize",
		       type = integer}
	       #column{name = "Subject",
		       type = string},
	       #column{name = "Read",
		       type = boolean}],

    Views = [#view{name = "view1",
		   primary_keys = ViewPKs,
		   %% 此视图将前两列 UserID 和 FromAddr 作为分页键
		   paging_key_len = 2,
		   columns = Columns}],
    
    Table1 = #table{name = ?TestTableName,
		   primary_keys = TablePKs,
		   %% 原表将前两列 UserID 和 ReceiveTime 作为分页键
		   paging_key_len = 2,
		   views = Views
		  },

%%    ?debugFmt("~n-------- table to create --------~n~p~n", [Table1]),
    Table1.
    
bad_pk_table_meta() ->
    %% View Primary Keys
    %% 视图的主键列，其中第一列：UserID为数据分片键
    %% 视图主键列包含原表的所有主键列，以保证唯一性
    %% 数据分片键必须和原表一致，其他列的顺序个数没有限制
    ViewPKs = [#primary_key{name = "UserID",
			    type = string},
	       #primary_key{name = "FromAddr",
			    type = integer},
	       #primary_key{name = "ReceiveTime",
			    type = string},
	       #primary_key{name = "MailSize",
			    type = integer}
	      ],
    
    Columns = [#column{name = "ToAddr",
		       type = string},
	       #column{name = "MailSize",
		       type = integer}
	       #column{name = "Subject",
		       type = string},
	       #column{name = "Read",
		       type = boolean}],

    Views = [#view{name = "view1",
		   primary_keys = ViewPKs,
		   %% 此视图将前两列 UserID 和 FromAddr 作为分页键
		   paging_key_len = 2,
		   columns = Columns}],
    
    #table{name = ?TestTableName,
	   primary_keys = "Bad Primary Keys",
	   %% 原表将前两列 UserID 和 ReceiveTime 作为分页键
	   paging_key_len = 2,
	   views = Views
	  }.

bad_view_table_meta() ->
    %% Table Primary Keys
    %% 表的主键列，其中第一列：UserID为数据分片键
    TablePKs = [#primary_key{name = "UserID",
			     type = string},
		#primary_key{name = "ReceiveTime",
			     type = string},
		#primary_key{name = "FromAddr",
			     type = integer}],
    
    %% View Primary Keys
    %% 视图的主键列，其中第一列：UserID为数据分片键
    %% 视图主键列包含原表的所有主键列，以保证唯一性
    %% 数据分片键必须和原表一致，其他列的顺序个数没有限制
    ViewPKs = [#primary_key{name = "UserID",
			    type = string},
	       #primary_key{name = "FromAddr",
			    type = integer},
	       #primary_key{name = "ReceiveTime",
			    type = string},
	       #primary_key{name = "MailSize",
			    type = integer}
	      ],
    
    Columns = [#column{name = "ToAddr",
		       type = string},
	       #column{name = "MailSize",
		       type = integer}
	       #column{name = "Subject",
		       type = string},
	       #column{name = "Read",
		       type = boolean}],
    
    BadViews = [#view{name = "bad_view",
		      primary_keys = ViewPKs,
		      %% 此视图将前两列 UserID 和 FromAddr 作为分页键
		      paging_key_len = bad_pkl,
		      columns = Columns}],
    
    #table{name = ?TestTableName,
	   primary_keys = TablePKs,
	   %% 原表将前两列 UserID 和 ReceiveTime 作为分页键
	   paging_key_len = 2,
	   views = BadViews
	  }.
    

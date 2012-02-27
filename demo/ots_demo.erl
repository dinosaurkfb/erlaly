%%%-------------------------------------------------------------------
%%% @author Kong Fanbin <kfbuniversity@gmail.com>
%%% @copyright (C) 2012, Kong Fanbin
%%% @doc
%%%
%%% @end
%%% Created : 20 Feb 2012 by Kong Fanbin <kfbuniversity@gmail.com>
%%%-------------------------------------------------------------------
-module(ots_demo).

-include("erlaly_ots.hrl").

-export([init/2,
	 demo_table_ops/0, demo_table_group_ops/0,
	 demo_bad_ops/0, demo_data_ops/0, 
	 demo_batch_data_ops/0, demo_transaction/0]).
-import(erlaly_util, [guess_type/1]).
-import(demo_util, [generate_col_rcds/2, generate_data_op/2]).

-define(TEST_TABLE_NAME, "TestCreateTable").
-define(TEST_TABLE_GROUP_NAME, "TestCreateTableGroup").

init(AccessId, AccessKey) ->
    erlaly:start(),
    ok = erlaly_ots:init(AccessId, AccessKey).

create_test_table() ->
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
    
    Table1 = #table{name = ?TEST_TABLE_NAME,
		   primary_keys = TablePKs,
		   %% 原表将前两列 UserID 和 ReceiveTime 作为分页键
		   paging_key_len = 2,
		   views = Views
		  },

    erlaly_ots:create_table(Table1).

demo_table_ops() ->
    {ok, Tables, _, _} = erlaly_ots:list_table(),
    io:format("~n-------- list_table --------~n~p~n", [Tables]),
    case lists:member(?TEST_TABLE_NAME, Tables) of
	true ->
	    {Result, Code, _, _} = erlaly_ots:delete_table(?TEST_TABLE_NAME);
	false ->
	    PrimaryKeys = [#primary_key{name = "pkey_str",
					type = string},
			   #primary_key{name = "pkey_bool",
					type = boolean},
			   #primary_key{name = "pkey_int",
					type = integer}],
	    
	    Table = #table{name = ?TEST_TABLE_NAME,
			   primary_keys = PrimaryKeys},

	    {ok, _, _, _} = erlaly_ots:create_table(Table),
	    io:format("~n-------- create_table ok --------~n", []),
	    {Result, Code, _, _} = erlaly_ots:delete_table(?TEST_TABLE_NAME)
    end,
    io:format("~n-------- delete_table return ~p --------~nCode:~p~n", [Result, Code]),
    Result1 = create_test_table(),
    case Result1 of
	{ok, _, _, _} ->
	    io:format("~n-------- create_table return ok --------~n", []),
	    {ok, TableR, {requestId, _}, {hostId, _}} = erlaly_ots:get_table_meta(?TEST_TABLE_NAME),
	    io:format("~n-------- table read --------------------~n~p~n", [TableR]);
	_ ->
	    io:format("~n-------- create_table return error --------~n", [])
    end.

demo_table_group_ops() ->
    {ok, TableGroups, {requestId, _RequestId}, {hostId, _HostId}} = erlaly_ots:list_table_group(),
    io:format("~n-------- list_table_group --------~n~p~n", [TableGroups]),

    {ok, Code, _, _} = erlaly_ots:create_table_group(?TEST_TABLE_GROUP_NAME, string),
    io:format("~n-------- create_table_group --------~n~p~n", [Code]),

    {ok, TableGroups1, _, _} = erlaly_ots:list_table_group(),
    io:format("~n-------- list_table_group --------~n~p~n", [TableGroups1]),

    {ok, Code, _, _} = erlaly_ots:delete_table_group(?TEST_TABLE_GROUP_NAME),
    io:format("~n-------- create_table_group --------~n~p~n", [Code]),

    {ok, TableGroups2, _, _} = erlaly_ots:list_table_group(),
    io:format("~n-------- list_table_group --------~n~p~n", [TableGroups2]).
    %% {ok, Table, {requestId, _}, {hostId, _}} = erlaly_ots:get_table_meta(?TEST_TABLE_NAME),
    %% io:format("~n-------- table meta --------~n~p~n", [Table]).
    
demo_bad_ops() ->
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
    
    Table1 = #table{name = ?TEST_TABLE_NAME,
		   primary_keys = "Bad Primary Keys",
		   %% 原表将前两列 UserID 和 ReceiveTime 作为分页键
		   paging_key_len = 2,
		   views = Views
		  },

    BadViews = [#view{name = "bad_view",
		      primary_keys = ViewPKs,
		      %% 此视图将前两列 UserID 和 FromAddr 作为分页键
		      paging_key_len = bad_pkl,
		      columns = Columns}],
    
    Table2 = #table{name = ?TEST_TABLE_NAME,
		   primary_keys = TablePKs,
		   %% 原表将前两列 UserID 和 ReceiveTime 作为分页键
		   paging_key_len = 2,
		   views = BadViews
		  },
    spawn(fun() -> erlaly_ots:create_table(Table1) end),
    spawn(fun() -> erlaly_ots:create_table(Table2) end).
    
demo_data_ops() ->
    Result = create_test_table(),
    case Result of
	{ok, _, _, _} ->
	    io:format("~n-------- create_table return ok --------~n", []),
	    data_ops();
	{error, {"OTSStorageObjectAlreadyExist", _, _, _}} ->
	    data_ops();
	{error, Desc} ->
	    io:format("~n-------- create_table return error --------~n~p~n", [Desc])
    end.
    
demo_transaction() ->
    Result = create_test_table(),
    case Result of
	{ok, _, _, _} ->
	    io:format("~n-------- create_table return ok --------~n", []),
	    transaction_ops();
	{error, {"OTSStorageObjectAlreadyExist", _, _, _}} ->
	    transaction_ops();
	{error, Desc} ->
	    io:format("~n-------- create_table return error --------~n~p~n", [Desc])
    end.

transaction_ops() ->
    {ok, TransactionID, _, _} = erlaly_ots:start_transaction(?TEST_TABLE_NAME, "U00001"),
    CommitResult = erlaly_ots:commit_transaction(TransactionID),
    io:format("CommitResult:~p~n", [CommitResult]),
    {ok, TransactionID1, _, _} = erlaly_ots:start_transaction(?TEST_TABLE_NAME, "U00001"),
    AbortResult = erlaly_ots:abort_transaction(TransactionID1),
    io:format("AbortResult:~p~n", [AbortResult]).

demo_batch_data_ops() ->
    Result = create_test_table(),
    case Result of
	{ok, _, _, _} ->
	    io:format("~n-------- create_table return ok --------~n", []),
	    batch_data_ops();
	{error, {"OTSStorageObjectAlreadyExist", _, _, _}} ->
	    batch_data_ops();
	{error, Desc} ->
	    io:format("~n-------- create_table return error --------~n~p~n", [Desc])
    end.

data_ops() ->
    %% Make sure the test table exists
    %% 确保测试的表存在
%%    create_test_table(),
    ColAttrs = [{true, "UserID"}, {true, "ReceiveTime"}, {true, "FromAddr"},
		{false, "ToAddr"}, {false, "MailSize"}, {false, "Subject"}, {false, "Read"}],
    MultiRowsData = [["U0001", "1998-1-1", "eric@demo.com", "bob@demo.com", 1000, "Hello", true],
		     ["U0001", "2011-10-20", "alice@demo.com", "bob@demo.com;vivian@demo.com", 15000, "Fw:greeting", true],
		     ["U0001", "2011-10-21", "alice@demo.com", "team1@demo.com", 8900, "Review", false],
		     ["U0002", "2010-3-18", "windy@demo.com", "tom@demo.com", 500, "Meeting Request", true]
		    ],
    %% 把多行数据和列属性映射成多行的column记录
    MultiRowColRcds = lists:map(fun(RowData) ->
					generate_col_rcds(ColAttrs, RowData)
				end, MultiRowsData),
    %% 把每一行column记录映射成一个put类型的data_op记录
    PutDataOps = [generate_data_op(RowColRcds, put) || RowColRcds <- MultiRowColRcds],
    io:format("PutDataOps:~p~n", [PutDataOps]),
    %% 进行put_data数据操作
    PutResults = put_data_ops(PutDataOps),
    io:format("PutResults:~p~n", [PutResults]),
    
    %% 获取每行指定列的数据
    GetRowDataOps = generate_get_row_data_ops(PutDataOps),
    RowResults = get_row_ops(GetRowDataOps),
    io:format("Get row results:~p~n", [RowResults]),

    %% 获取每行的所有列的数据
    GetRowAllDataOps = generate_get_row_data_ops_all(PutDataOps),
    RowAllResults = get_row_ops(GetRowAllDataOps),
    io:format("Get row all columns results:~p~n", [RowAllResults]),

    %% 把每一行column记录映射成一个data_op记录
    DeleteDataOps = [generate_data_op(RowColRcds, delete) || RowColRcds <- MultiRowColRcds],
    DeleteResults = delete_data_ops(DeleteDataOps),
    
    io:format("Delete data results:~p~n", [DeleteResults]),
    ok.

batch_data_ops() ->
    %% Make sure the test table exists
    %% 确保测试的表存在
%%    create_test_table(OTS),
    ColAttrs = [{true, "UserID"}, {true, "ReceiveTime"}, {true, "FromAddr"},
		{false, "ToAddr"}, {false, "MailSize"}, {false, "Subject"}, {false, "Read"}],
    MultiRowsData = [["U0001", "1998-1-1", "eric@demo.com", "bob@demo.com", 1000, "Hello", true],
		     ["U0001", "2011-10-20", "alice@demo.com", "bob@demo.com;vivian@demo.com", 15000, "Fw:greeting", true],
		     ["U0001", "2011-10-21", "alice@demo.com", "team1@demo.com", 8900, "Review", false],
		     ["U0002", "2010-3-18", "windy@demo.com", "tom@demo.com", 500, "Meeting Request", true]
		    ],
    %% 把多行数据和列属性映射成多行的column记录
    MultiRowColRcds = lists:map(fun(RowData) ->
					generate_col_rcds(ColAttrs, RowData)
				end, MultiRowsData),
    %% 把每一行column记录映射成一个put类型的data_op记录
    PutDataOps = [generate_data_op(RowColRcds, put) || RowColRcds <- MultiRowColRcds],
    io:format("PutDataOps:~p~n", [PutDataOps]),
    %% 获取每行指定列的数据
    GetRowDataOps = generate_get_row_data_ops(PutDataOps),

    %% 把每一行column记录映射成一个data_op记录
    DeleteDataOps = [generate_data_op(RowColRcds, delete) || RowColRcds <- MultiRowColRcds],
    
    %% 批量添加数据
    BatchPutResult = batch_modify_data_ops(PutDataOps),
    io:format("BatchPutResult:~p~n", [BatchPutResult]),
    
    %% 按范围读取数据
    %% 以第一行的主键值为条件(前几个PK列与第一行相同，最后一个PK列选择所有值)来进行多行查询
    RangeGetResults = get_rows_by_range_ops(hd(GetRowDataOps)),
    io:format("RangeGetResult:~p~n", [RangeGetResults]),

    %% 按范围读取数据
    %% 获取每行的所有列的数据
    GetRowAllDataOps = generate_get_row_data_ops_all(PutDataOps),
    RangeGetAllResults = get_rows_by_range_ops(hd(GetRowAllDataOps)),
    io:format("RangeGetAllResults:~p~n", [RangeGetAllResults]),

    %% 批量删除数据
    BatchDeleteResult = batch_modify_data_ops(DeleteDataOps),
    io:format("BatchDeleteResult:~p~n", [BatchDeleteResult]),
    ok.

get_rows_by_range_ops(#data_op{primary_keys = PrimaryKeys} = DataOp) ->
    %% 得到该行的 DataOp 以及这一行所有的 PrimaryKeys

    %% 将最后一个 PrimaryKey 加上 range 属性
    %% 去掉 value 和 type 属性
    ReversePKs = lists:reverse(PrimaryKeys),
    [#primary_key{type = Type, name = Name} | OtherPKs ] = ReversePKs,
    RangingPK = #primary_key{name = Name,
			     range = #range{rtype = Type,
					    rbegin = inf_min,
					    rend = inf_max}},
    NewDataOp = DataOp#data_op{primary_keys = lists:reverse([RangingPK | OtherPKs])},
    erlaly_ots:get_rows_by_range(?TEST_TABLE_NAME, NewDataOp).

batch_modify_data_ops(TestRowDataOps) ->
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

    lists:map(
      fun({PartitionKeyValue, DataOps}) ->
	      {ok, TransactionID, _, _} = erlaly_ots:start_transaction(?TEST_TABLE_NAME, PartitionKeyValue),
	      io:format("TransactionID=~p~n", [TransactionID]),
	      %% 保证同一数据分片键内的 data_op 记录的顺序与原来一致
	      %% 如果要求不同数据分片键的 data_op 记录顺序与原来一致，那么此函数不适用
	      {ok, _, _, _} = erlaly_ots:batch_modify_data(?TEST_TABLE_NAME, lists:reverse(DataOps), TransactionID),
	      erlaly_ots:commit_transaction(TransactionID)
      end, BatchDataOps).

get_row_ops(TestDataOps) ->
    lists:map(fun(X) ->
		      erlaly_ots:get_row(?TEST_TABLE_NAME, X)
	      end, TestDataOps).

put_data_ops(TestDataOps) ->
    lists:map(fun(X) ->
		      erlaly_ots:put_data(?TEST_TABLE_NAME, X)
	      end, TestDataOps).

%% 这是一种偷懒的做法，PutDataOps必须是之前用于put_data的data_op记录列表
generate_get_row_data_ops(PutDataOps) ->
    lists:map(fun(#data_op{columns = Columns} = DataOp) ->
		      NewColumns = [Col#column{value = undefined,
					       type = undefined} || Col <- Columns],
		      DataOp#data_op{columns = NewColumns}
	      end, PutDataOps).

%% 这是一种偷懒的做法，PutDataOps必须是之前用于put_data的data_op记录列表
%% 获取所有列的数据
generate_get_row_data_ops_all(PutDataOps) ->
    lists:map(fun(DataOp) ->
		      DataOp#data_op{columns = undefined}
	      end, PutDataOps).

delete_data_ops(TestDataOps) ->
    lists:map(fun(X) ->
		      erlaly_ots:delete_data(?TEST_TABLE_NAME, X)
	      end, TestDataOps).


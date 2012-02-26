%%%-------------------------------------------------------------------
%%% @author Kong Fanbin <kfbuniversity@gmail.com>
%%% @copyright (C) 2012, Kong Fanbin
%%% @doc This is an client implementation for Aliyun's OTS WebService
%% 
%%% @end
%%% Created : 16 Feb 2012 by Kong Fanbin <kfbuniversity@gmail.com>
%%%-------------------------------------------------------------------
-module(erlaly_ots).

%% exports
-export([create_table/1, list_table/0, delete_table/1, get_table_meta/1,
	 create_table_group/2, list_table_group/0, delete_table_group/1,
	 start_transaction/2, commit_transaction/1,
	 abort_transaction/1, batch_modify_data/3,
	 put_data/3, delete_data/3, 
	 put_data/2, delete_data/2, modify_data/2,
	 get_row/3, get_rows_by_range/3, get_rows_by_offset/3,
	 get_row/2, get_rows_by_range/2, get_rows_by_offset/2,
	 init/2
	]).

%% include record definitions
-include_lib("xmerl/include/xmerl.hrl").
-include("erlaly_ots.hrl").

-define(ALY_OTS_HOST, "service.ots.aliyun.com").
-define(API_VERSION, "1").
    
%%--------------------------------------------------------------------
%% @doc Initialize OTS access information.
%% 
%% The provided access id and access key are stored in an ets table 
%% named as "erlaly_ots" which is the module name. Make sure this name
%% is not used elsewhere. As the ets table is global, erlaly_ots does
%% not support requests from different access ids simultaneously.
%%
%% @spec init(string(), string())-> ok
%% @end
%%--------------------------------------------------------------------
-spec init(OTS_ACCESS_ID :: string(), OTS_ACCESS_KEY :: string()) -> ok | already_inited.

init(OTS_ACCESS_ID, OTS_ACCESS_KEY) when is_list(OTS_ACCESS_ID),
					 is_integer(hd(OTS_ACCESS_ID)),
					 is_list(OTS_ACCESS_KEY),
					 is_integer(hd(OTS_ACCESS_KEY)) ->
    case ets:info(?MODULE) of
	undefined ->
	    _Tid = ets:new(?MODULE, [set, protected, named_table, {read_concurrency, true}]);
	_ ->
	    ok
    end,
    
    true = ets:insert(?MODULE, {ots_access_id, OTS_ACCESS_ID}),
    true = ets:insert(?MODULE, {ots_access_key, OTS_ACCESS_KEY}),
    ok.

%%--------------------------------------------------------------------
%% @doc Create a table in OTS.
%% 
%% At least table name and primary keys are required to create a table
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec create_table(Table) -> Result
%%   Table = table()
%%   Result = {ok, Code, RequestId, HostId} | {error, Desc}
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec create_table(Table :: table()) ->
			  {ok, Code :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
			  {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

create_table(#table{name = TableName,
		    primary_keys = PrimaryKeys,
		    paging_key_len = PagingKeyLen,
		    views = Views,
		    group_name = GroupName}) when is_list(TableName),
						  is_integer(hd(TableName)),
						  is_list(PrimaryKeys),
						  is_record(hd(PrimaryKeys), primary_key) ->
    PKParams = get_pk_params(PrimaryKeys),
%%    io:format("PKParams: ~p~n", [PKParams]),

    OptionalParams = attrs_to_params("", [], 
				     [{paging_key_len, PagingKeyLen},
				      {views, Views},
				      {group_name, GroupName}
				      ]),

%%    io:format("OptionalParams: ~p~n", [OptionalParams]),
    Params = [OptionalParams |
	      [PKParams |
	       [{"TableName", TableName}]]],
	    
    try genericRequest("CreateTable", lists:flatten(Params)) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=Code}] =
		xmerl_xpath:string("/CreateTableResult/Code/text()", XmlDoc), 
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("/CreateTableResult/RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("/CreateTableResult/HostID/text()", XmlDoc), 
	    {ok, Code, {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%% @doc List all tables in OTS.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% 
%% @spec list_table() -> Result
%%   Result = {ok, Tables, RequestId, HostId} | {error, Desc}
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   TableNames = [string()]
%% @end
%%--------------------------------------------------------------------
-spec list_table() ->
			{ok, TableNames :: [string()], {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
			{error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

list_table() ->	    
    try genericRequest("ListTable", []) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    TableNodes = xmerl_xpath:string("/ListTableResult/TableNames/TableName/text()", XmlDoc), 
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("/ListTableResult/RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("/ListTableResult/HostID/text()", XmlDoc), 
	    {ok, [Node#xmlText.value || Node <- TableNodes], {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%% @doc List all table groups in OTS.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% 
%% @spec list_table_group() -> Result
%%   Result = {ok, TableGroups, RequestId, HostId} | {error, Desc}
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   TableGroups = [string()]
%% @end
%%--------------------------------------------------------------------
-spec list_table_group() ->
			      {ok, Groups :: [string()], {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
			      {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

list_table_group() ->	    
    try genericRequest("ListTableGroup", []) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    GroupNodes = xmerl_xpath:string("/ListTableGroupResult/TableGroupNames/TableGroupName/text()", XmlDoc), 
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("/ListTableGroupResult/RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("/ListTableGroupResult/HostID/text()", XmlDoc), 
	    {ok, [Node#xmlText.value || Node <- GroupNodes], {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%% @doc Delete a table group in OTS.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% 
%% @spec delete_table_group(GroupName) -> Result
%%   GroupName = string()
%%   Result = {ok, Code, RequestId, HostId} | {error, Desc}
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec delete_table_group(GroupName :: string()) ->
				{ok, Code :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
				{error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

delete_table_group(GroupName) when is_list(GroupName),
				   is_integer(hd(GroupName)) ->
    try genericRequest("DeleteTableGroup", [{"TableGroupName", GroupName}]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=Code}] =
		xmerl_xpath:string("/DeleteTableGroupResult/Code/text()", XmlDoc), 
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("/DeleteTableGroupResult/RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("/DeleteTableGroupResult/HostID/text()", XmlDoc), 
	    {ok, Code, {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%% @doc Create a table group in OTS.
%% 
%% A table group name and partition key type are required to create a
%% table group.
%% Partition key type can only be integer or string.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%%
%% @spec create_table_group(GroupName, PartitionKeyType) -> Result
%%   GroupName = string(),
%%   PartitionKeyType = atom(),
%%   Result = {ok, Code, RequestId, HostId} | {error, Desc}
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec create_table_group(GroupName :: string(), PartitionKeyType :: ots_partition_key_t()) ->
				{ok, Code :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
				{error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

create_table_group(GroupName, PartitionKeyType) when is_list(GroupName),
						     is_integer(hd(GroupName)) ->
    try genericRequest("CreateTableGroup",
		       [{"PartitionKeyType", get_type_str(PartitionKeyType)},
			{"TableGroupName", GroupName}]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=Code}] =
		xmerl_xpath:string("/CreateTableGroupResult/Code/text()", XmlDoc), 
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("/CreateTableGroupResult/RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("/CreateTableGroupResult/HostID/text()", XmlDoc), 
	    {ok, Code, {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%% @doc Delete a table in OTS.
%% 
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%%
%% @spec delete_table(TableName) -> Result
%%   TableName = string()
%%   Result = {ok, Code, RequestId, HostId} | {error, Desc}
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec delete_table(TableName :: string()) ->
			  {ok, Code :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
			  {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

delete_table(TableName) ->
    try genericRequest("DeleteTable", [{"TableName", TableName}]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=Code}] =
		xmerl_xpath:string("/DeleteTableResult/Code/text()", XmlDoc), 
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("/DeleteTableResult/RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("/DeleteTableResult/HostID/text()", XmlDoc), 
	    {ok, Code, {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%% @doc Get a table's META in OTS.
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%%
%% @spec get_table_meta(TableName) -> Result
%%   TableName = string()
%%   Result = {ok, Table, RequestId, HostId} | {error, Desc}
%%   Table = table()
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec get_table_meta(TableName :: string()) ->
			    {ok, Table :: table(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
			    {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

get_table_meta(TableName) ->
    try genericRequest("GetTableMeta", [{"TableName", TableName}]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    Table = get_table_record(XmlDoc),
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("/GetTableMetaResult/RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("/GetTableMetaResult/HostID/text()", XmlDoc), 
	    {ok, Table, {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%% @doc Start a transaction in OTS table or table group.
%% 
%% A table or table group name and a partition key value are required 
%% to start a transaction.
%% Partition key value can only be integer or string type.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%%
%% @spec start_transaction(EntityName, PartitionKeyValue) -> Result
%%   EntityName = string()
%%   PartitionKeyValue = string() | integer()
%%   Result = {ok, TransactionID, RequestId, HostId} | {error, Desc}
%%   TransactionID = string()
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------

-spec start_transaction(EntityName :: string(), PartitionKeyValue :: integer() | string()) ->
			       {ok, TransactionID :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
			       {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

start_transaction(EntityName, PartitionKeyValue) when is_list(EntityName),
						      is_integer(hd(EntityName)),
						      is_integer(PartitionKeyValue) ->
    Params = [{"PartitionKeyType", "INTEGER"},
	      {"PartitionKeyValue", integer_to_list(PartitionKeyValue)},
	      {"EntityName", EntityName}],
    start_transaction(Params);
start_transaction(EntityName, PartitionKeyValue) when is_list(EntityName),
						      is_integer(hd(EntityName)),
						      is_list(PartitionKeyValue),
						      is_integer(hd(PartitionKeyValue)) ->
    Params = [{"PartitionKeyType", "STRING"},
	      {"PartitionKeyValue", "'" ++ PartitionKeyValue ++ "'"},
	      {"EntityName", EntityName}],
    start_transaction(Params).

start_transaction(Params) ->
    try genericRequest("StartTransaction", Params) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=TransactionID}] =
		xmerl_xpath:string("/StartTransactionResult/TransactionID/text()", XmlDoc), 
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("/StartTransactionResult/RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("/StartTransactionResult/HostID/text()", XmlDoc), 
	    {ok, TransactionID, {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%% @doc Commit a transaction.
%% 
%% The transaction ID will be invalid after a successful commit.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec commit_transaction(TransactionID) -> Result
%%   TransactionID = string()
%%   Result = {ok, Code, RequestId, HostId} | {error, Desc}
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec commit_transaction(TransactionID :: string()) ->
				{ok, Code :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
				{error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

commit_transaction(TransactionID) when is_list(TransactionID),
				      is_integer(hd(TransactionID)) ->
    try genericRequest("CommitTransaction", [{"TransactionID", TransactionID}]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=Code}] =
		xmerl_xpath:string("/CommitTransactionResult/Code/text()", XmlDoc), 
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("/CommitTransactionResult/RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("/CommitTransactionResult/HostID/text()", XmlDoc), 
	    {ok, Code, {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%% @doc Abort a transaction.
%% 
%% All operations during this transaction will be canceled.
%% The transaction ID will be invalid after this transaction abort.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec abort_transaction(TransactionID) -> Result
%%   TransactionID = string()
%%   Result = {ok, Code, RequestId, HostId} | {error, Desc}
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec abort_transaction(TransactionID :: string()) ->
			       {ok, Code :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
			       {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

abort_transaction(TransactionID) when is_list(TransactionID),
				     is_integer(hd(TransactionID)) ->
    try genericRequest("AbortTransaction", [{"TransactionID", TransactionID}]) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=Code}] =
		xmerl_xpath:string("/AbortTransactionResult/Code/text()", XmlDoc), 
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("/AbortTransactionResult/RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("/AbortTransactionResult/HostID/text()", XmlDoc), 
	    {ok, Code, {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%% @doc Put a row of data in OTS.
%% 
%% At least table name and data operation are required to put data
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec put_data(TableName, DataOp, TransactionID) -> Result
%%   TableName = string()
%%   DataOp = data_op()
%%   TransactionID = string()
%%   Result = {ok, Code, RequestId, HostId} | {error, Desc}
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec put_data(TableName :: string(), DataOp :: data_op(), TransactionID :: string()) ->
		      {ok, Code :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
		      {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

put_data(TableName, 
	 #data_op{primary_keys = PrimaryKeys,
		  columns = Columns,
		  checking = Checking},
	 TransactionID) when is_list(PrimaryKeys),
			     is_record(hd(PrimaryKeys), primary_key),
			     is_list(TransactionID),
			     is_integer(hd(TransactionID)) ->
    Params = attrs_to_params("", [], 
			     [{table_name, TableName},
			      {primary_keys, PrimaryKeys},
			      {columns, Columns},
			      {checking, Checking},
			      {transaction_id, TransactionID}
			     ]),
    modify_data("PutData", Params).

%%--------------------------------------------------------------------
%% @doc Put a row of data in OTS.
%% 
%% At least table name and data operation are required to put data
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec put_data(TableName, DataOp) -> Result
%%   TableName = string()
%%   DataOp = data_op()
%%   Result = {ok, Code, RequestId, HostId} | {error, Desc}
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec put_data(TableName :: string(), DataOp :: data_op()) ->
		      {ok, Code :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
		      {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

put_data(TableName, #data_op{primary_keys = PrimaryKeys,
			     columns = Columns,
			     checking = Checking}) when is_list(PrimaryKeys),
							is_record(hd(PrimaryKeys), primary_key) ->
    Params = attrs_to_params("", [], 
			     [{table_name, TableName},
			      {primary_keys, PrimaryKeys},
			      {columns, Columns},
			      {checking, Checking}
			     ]),
    modify_data("PutData", Params).

%%--------------------------------------------------------------------
%% @doc Get a row of data in OTS.
%% 
%% At least table name and data operation are required to get data
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec get_row(TableName, DataOp, TransactionID) -> Result
%%   TableName = string()
%%   DataOp = data_op()
%%   TransactionID = string()
%%   Result = {ok, Columns, RequestId, HostId} | {error, Desc}
%%   Columns = [column()]
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   Code = string()
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec get_row(TableName :: string(), DataOp :: data_op(), TransactionID :: string()) ->
		     {ok, [Column :: column()], {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
		     {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

get_row(TableName, 
	#data_op{primary_keys = PrimaryKeys,
		 columns = Columns},
	TransactionID) when is_list(PrimaryKeys),
			    is_record(hd(PrimaryKeys), primary_key),
			    is_list(TransactionID),
			    is_integer(hd(TransactionID)) ->
    Params = attrs_to_params("", [], 
			     [{table_name, TableName},
			      {primary_keys, PrimaryKeys},
			      {columns, Columns},
			      {transaction_id, TransactionID}
			     ]),
    read_data("GetRow", Params).

%%--------------------------------------------------------------------
%% @doc Get a row of data in OTS.
%% 
%% At least table name and data operation are required to get data
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec get_row(TableName, DataOp) -> Result
%%   TableName = string()
%%   DataOp = data_op()
%%   Result = {ok, Columns, RequestId, HostId} | {error, Desc}
%%   Columns = [column()]
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec get_row(TableName :: string(), DataOp :: data_op()) ->
		     {ok, [Column :: column()], {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
		     {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

get_row(TableName, #data_op{primary_keys = PrimaryKeys,
			    columns = Columns}) when is_list(PrimaryKeys),
						     is_record(hd(PrimaryKeys), primary_key) ->
    Params = attrs_to_params("", [], 
			     [{table_name, TableName},
			      {primary_keys, PrimaryKeys},
			      {columns, Columns}
			     ]),
    read_data("GetRow", Params).

%%--------------------------------------------------------------------
%% @doc Get multi rows of data from OTS table or views by offset.
%% 
%% At least table name and data operation are required to get data
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec get_rows_by_offset(TableName, DataOp, TransactionID) -> Result
%%   TableName = string()
%%   DataOp = data_op()
%%   TransactionID = string()
%%   Result = {ok, Columns, RequestId, HostId} | {error, Desc}
%%   Columns = [column()]
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec get_rows_by_offset(TableName :: string(), DataOp :: data_op(), TransactionID :: string()) ->
				{ok, [Column :: column()], {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
				{error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

get_rows_by_offset(TableName, 
		   #data_op{pagings = Pagings,
			    columns = Columns,
			    offset = Offset,
			    top = Top},
		   TransactionID) when is_list(Pagings),
				       is_record(hd(Pagings), paging),
				       is_list(TransactionID),
				       is_integer(hd(TransactionID)),
				       is_integer(Offset),
				       is_integer(Top) ->
    Params = attrs_to_params("", [], 
			     [{table_name, TableName},
			      {pagings, Pagings},
			      {columns, Columns},
			      {offset, Offset},
			      {top, Top},
			      {transaction_id, TransactionID}
			     ]),
    read_data("GetRowsByOffset", Params).

%%--------------------------------------------------------------------
%% @doc Get multi rows of data from OTS table or views by range.
%% 
%% At least table name and data operation are required to get data
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec get_rows_by_offset(TableName, DataOp) -> Result
%%   TableName = string()
%%   DataOp = data_op()
%%   Result = {ok, Columns, RequestId, HostId} | {error, Desc}
%%   Columns = [column()]
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec get_rows_by_offset(TableName :: string(), DataOp :: data_op()) ->
				{ok, [Column :: column()], {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
				{error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

get_rows_by_offset(TableName, 
		   #data_op{pagings = Pagings,
			    columns = Columns,
			    offset = Offset,
			    top = Top}) when is_list(Pagings),
					     is_record(hd(Pagings), paging),
					     is_integer(Offset),
					     is_integer(Top) ->
    Params = attrs_to_params("", [], 
			     [{table_name, TableName},
			      {pagings, Pagings},
			      {columns, Columns},
			      {offset, Offset},
			      {top, Top}
			     ]),
    read_data("GetRowsByOffset", Params).

%%--------------------------------------------------------------------
%% @doc Get multi rows of data from OTS table or views by range.
%% 
%% At least table name and data operation are required to get data
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec get_rows_by_range(TableName, DataOp, TransactionID) -> Result
%%   TableName = string()
%%   DataOp = data_op()
%%   TransactionID = string()
%%   Result = {ok, Columns, RequestId, HostId} | {error, Desc}
%%   Columns = [column()]
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec get_rows_by_range(TableName :: string(), DataOp :: data_op(), TransactionID :: string()) ->
			       {ok, [Column :: column()], {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
			       {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

get_rows_by_range(TableName, 
		  #data_op{primary_keys = PrimaryKeys,
			   columns = Columns,
			   top = Top},
		  TransactionID) when is_list(PrimaryKeys),
				      is_record(hd(PrimaryKeys), primary_key),
				      is_list(TransactionID),
				      is_integer(hd(TransactionID)) ->
    Params = attrs_to_params("", [], 
			     [{table_name, TableName},
			      {primary_keys, PrimaryKeys},
			      {columns, Columns},
			      {top, Top},
			      {transaction_id, TransactionID}
			     ]),
    read_data("GetRowsByRange", Params).

%%--------------------------------------------------------------------
%% @doc Get multi rows of data from OTS table or views by range.
%% 
%% At least table name and data operation are required to get data
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec get_rows_by_range(TableName, DataOp) -> Result
%%   TableName = string()
%%   DataOp = data_op()
%%   Result = {ok, Columns, RequestId, HostId} | {error, Desc}
%%   Columns = [column()]
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec get_rows_by_range(TableName :: string(), DataOp :: data_op()) ->
				{ok, [Column :: column()], {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
				{error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

get_rows_by_range(TableName,
		  #data_op{primary_keys = PrimaryKeys,
			   columns = Columns,
			   top = Top}) when is_list(PrimaryKeys),
					    is_record(hd(PrimaryKeys), primary_key) ->
    Params = attrs_to_params("", [], 
			     [{table_name, TableName},
			      {primary_keys, PrimaryKeys},
			      {columns, Columns},
			      {top, Top}
			     ]),
    read_data("GetRowsByRange", Params).

read_data(Request, Params) when is_list(Params) ->
    try genericRequest(Request, lists:flatten(Params)) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    Rows = get_row_record_list(XmlDoc),
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("//RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("//HostID/text()", XmlDoc), 
	    {ok, Rows, {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

get_row_record_list(XmlDoc) ->
    TNames = xmerl_xpath:string("//Table/@name", XmlDoc),
    case TNames of
	[] ->
	    [];
	[#xmlAttribute{value=TableName}] ->
%%	    io:format("TableName: ~p~n", [TableName]),
	    Rows = xmerl_xpath:string("//Table/Row", XmlDoc),
	    [get_row_record(Row) || Row <- Rows]
    end.

get_row_record(RowDoc) ->
    Cols = xmerl_xpath:string("/Row/Column", RowDoc),
    [get_col_record(Col) || Col <- Cols].

get_col_record(ColDoc) ->
    PKCols = xmerl_xpath:string("/Column[@PK='true']", ColDoc),
    IsPK = case PKCols of
	       [] ->
		   false;
	       _ ->
		   true
	   end,
    [#xmlText{value=Name}] = xmerl_xpath:string("//Name/text()", ColDoc),
    Types = xmerl_xpath:string("//Value/@type", ColDoc),
    Type = case Types of
	       [] ->
		   undefined;
	       [#xmlAttribute{value=T}] ->
		   get_type_atom(T)
	   end,
	    
    Values = xmerl_xpath:string("//Value/text()", ColDoc),
    Value = case {Values, Type} of
		{[], _} ->
		    undefined;
		{_, undefined} ->
		    undefined;
		{[#xmlText{value=V}], integer} ->
		    list_to_integer(V);
		{[#xmlText{value=V}], double} ->
		    list_to_float(V);
		{[#xmlText{value=V}], boolean} ->
		    list_to_boolean(V);
		{[#xmlText{value=V}], string} ->
		    V
	    end,
    #column{is_pk = IsPK, name = Name, type = Type, value = Value}.

%%--------------------------------------------------------------------
%% @doc Delete a row of data in OTS.
%% 
%% At least table name and data operation are required to delete data
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec delete_data(TableName, DataOp, TransactionID) -> Result 
%%   TableName = string()
%%   DataOp = data_op()
%%   TransactionID = string()
%%   Result = {ok, Code, RequestId, HostId} | {error, Desc}
%%   Code = string()
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec delete_data(TableName :: string(), DataOp :: data_op(), TransactionID :: string()) ->
			 {ok, Code :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
			 {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

delete_data(TableName, 
	    #data_op{primary_keys = PrimaryKeys,
		     columns = Columns},
	    TransactionID) when is_list(PrimaryKeys),
				is_record(hd(PrimaryKeys), primary_key),
				is_list(TransactionID),
				is_integer(hd(TransactionID)) ->
    Params = attrs_to_params("", [], 
			     [{table_name, TableName},
			      {primary_keys, PrimaryKeys},
			      {columns, Columns},
			      {transaction_id, TransactionID}
			     ]),
    modify_data("DeleteData", Params).

%%--------------------------------------------------------------------
%% @doc Delete a row of data in OTS.
%% 
%% At least table name and data operation are required to delete data
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec delete_data(TableName, DataOp) -> Result 
%%   TableName = string()
%%   DataOp = data_op()
%%   Result = {ok, Code, RequestId, HostId} | {error, Desc}
%%   Code = string()
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec delete_data(TableName :: string(), DataOp :: data_op()) ->
			 {ok, Code :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
			 {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

delete_data(TableName, #data_op{primary_keys = PrimaryKeys,
				columns = Columns}) when is_list(PrimaryKeys),
							 is_record(hd(PrimaryKeys), primary_key) ->
    Params = attrs_to_params("", [], 
			     [{table_name, TableName},
			      {primary_keys, PrimaryKeys},
			      {columns, Columns}
			     ]),
    modify_data("DeleteData", Params).

%%--------------------------------------------------------------------
%% @doc Multiple put_data or delete_data operations in one request.
%% 
%% Table name and data operation list and transaction id are required
%% to finish this request.
%% The table name must be unique among the tables associated with your
%% aliyun Access ID.
%%
%%
%% See document:
%% http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_01_06.pdf
%% @spec batch_modify_data(TableName, [DataOp], TransactionID) -> Result
%%   TableName = string()
%%   DataOp = data_op()
%%   TransactionID = string()
%%   Result = {ok, Code, RequestId, HostId} | {error, Desc}
%%   Code = string()
%%   Desc = {ErrorCode, ErrorMsg, RequestId, HostId}
%%   RequestId = {requestId, string()}
%%   HostId = {hostId, string()}
%%   ErrorCode = string()
%%   ErrorMsg = string()
%% @end
%%--------------------------------------------------------------------
-spec batch_modify_data(TableName :: string(), [DataOp :: data_op()], TransactionID :: string()) ->
			       {ok, Code :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}} |
			       {error, {ErrorCode :: string(), {requestId, RequestID :: string()}, {hostId, HostID :: string()}}}.

batch_modify_data(TableName, DataOps, TransactionID) when is_list(DataOps),
							  is_record(hd(DataOps), data_op),
							  is_list(TransactionID),
							  is_integer(hd(TransactionID)) ->
    
    Params = attrs_to_params("", [], 
			     [{table_name, TableName},
			      {modify, DataOps},
			      {transaction_id, TransactionID}
			     ]),
    modify_data("BatchModifyData", Params).

modify_data(Request, Params) when is_list(Params) ->
    try genericRequest(Request, lists:flatten(Params)) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=Code}] =
		xmerl_xpath:string("//Code/text()", XmlDoc), 
	    [#xmlText{value=RequestID}] =
		xmerl_xpath:string("//RequestID/text()", XmlDoc), 
	    [#xmlText{value=HostID}] =
		xmerl_xpath:string("//HostID/text()", XmlDoc), 
	    {ok, Code, {requestId, RequestID}, {hostId, HostID}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

get_pk_record(PK) ->
    [#xmlText{value=Name}] = xmerl_xpath:string("//Name/text()", PK),
    [#xmlText{value=Type}] = xmerl_xpath:string("//Type/text()", PK),
    #primary_key{name = Name, type = get_type_atom(Type)}.

get_column_record(Column) ->
    [#xmlText{value=Name}] = xmerl_xpath:string("//Name/text()", Column),
    [#xmlText{value=Type}] = xmerl_xpath:string("//Type/text()", Column),
    #column{name = Name, type = get_type_atom(Type)}.

get_view_record(View) ->
    [#xmlText{value=Name}] = xmerl_xpath:string("/View/Name/text()", View),
    PKDocs = xmerl_xpath:string("/View/PrimaryKey", View),
    ColumnDocs = xmerl_xpath:string("/View/Column", View),
    PKLDocs = xmerl_xpath:string("/View/PagingKeyLen/text()", View),
    PagingKeyLen = case PKLDocs of
		       [] ->
			   undefined;
		       [P] ->
			   list_to_integer(P#xmlText.value)
		   end,

    Columns = case ColumnDocs of
		  [] ->
		      undefined;
		  _ ->
		      [get_column_record(ColumnDoc) || ColumnDoc <- ColumnDocs]
	      end,
    
    #view{name = Name,
	  primary_keys = [get_pk_record(PKDoc) || PKDoc <- PKDocs],
	  columns = Columns,
	  paging_key_len = PagingKeyLen}.


get_table_record(XmlDoc) ->
    [TableMeta] = xmerl_xpath:string("/GetTableMetaResult/TableMeta[1]", XmlDoc), 
    [#xmlText{value=TableName}] = xmerl_xpath:string("//TableName/text()", TableMeta),
%%    io:format("TableName: ~p~n", [TableName]),
    PKs = xmerl_xpath:string("/TableMeta/PrimaryKey", TableMeta),
    Views = xmerl_xpath:string("/TableMeta/View", TableMeta),
    PKLDocs = xmerl_xpath:string("/TableMeta/PagingKeyLen/text()", TableMeta),
    PagingKeyLen = case PKLDocs of
		       [] ->
			   undefined;
		       [P] ->
			   list_to_integer(P#xmlText.value)
		   end,
    TGNDocs = xmerl_xpath:string("/TableMeta/TableGroupName/text()", TableMeta),
    GroupName = case TGNDocs of
			 [] ->
			     undefined;
			 [TGN] ->
			     TGN#xmlText.value
		     end,
    #table{views = [get_view_record(View) || View <- Views],
	   primary_keys = [get_pk_record(PK) || PK <- PKs],
	   name = TableName, 
	   paging_key_len = PagingKeyLen,
	   group_name = GroupName}.

%%--------------------------------------------------------------------
%% @doc Get modify parameters from column record list
%% @spec get_modify_params(Prefix :: string(),
%%                         DataOps :: [Column :: column()]) ->
%%       [{Param :: string(), Value :: string()} |
%%        {Param :: string(), Value :: binary()}]
%% @end
%%--------------------------------------------------------------------
get_modify_params(_, []) ->
    [];
get_modify_params(Prefix, DataOps) when is_list(DataOps),
					is_record(hd(DataOps), data_op) ->
    {_, ModifyParams} = lists:foldl(
			  fun(DataOp, {N, AccIn}) ->
				  Prefix1 = Prefix ++ "Modify." ++ integer_to_list(N) ++ ".",
				  #data_op{mtype = MType,
					   primary_keys = PrimaryKeys,
					   columns = Columns,
					   checking = Checking} = DataOp,
				  case MType of
				      put ->
					  {N+1, attrs_to_params(Prefix1, 
								AccIn,
								[{mtype, MType},
								 {primary_keys, PrimaryKeys},
								 {columns, Columns},
								 {checking, Checking}
								])};
				      delete ->
					  {N+1, attrs_to_params(Prefix1, 
								AccIn,
								[{mtype, MType},
								 {primary_keys, PrimaryKeys},
								 {columns, Columns}
								])};
				      _ ->
					  throw({error, "Modify type should be set as put or delete"})
				  end
			  end,
			  {1, []}, DataOps),
    ModifyParams.
    
%%--------------------------------------------------------------------
%% @doc Get column parameters from column record list
%% @spec get_column_params(Prefix :: string(),
%%                         Columns :: [Column :: column()]) ->
%%       [{Param :: string(), Value :: string()} |
%%        {Param :: string(), Value :: binary()}]
%% @end
%%--------------------------------------------------------------------
get_column_params(Prefix, Columns) when is_list(Columns),
					is_record(hd(Columns), column) ->
    {_, ColumnParams} = lists:foldl(
		      fun(Column, {N, AccIn}) ->
			      Prefix1 = Prefix ++ "Column." ++ integer_to_list(N) ++ ".",
			      #column{name = Name, type = Type, value = Value} = Column,
			      {N+1, attrs_to_params(Prefix1, 
						    AccIn,
						    [{name, Name}, {value, Value}, {type, Type}])}
		      end,
		      {1, []}, Columns),
    ColumnParams.
    
%%--------------------------------------------------------------------
%% @doc Get range parameters from range record list
%% @spec get_pk_range_params(Prefix :: string(),
%%                     PKRange :: [PKRange :: range()]) ->
%%       [{Param :: string(), Value :: string()} |
%%        {Param :: string(), Value :: binary()}]
%% @end
%%--------------------------------------------------------------------
get_pk_range_params(Prefix, PKRange) when is_record(PKRange, range) ->
    #range{rtype = Type, rbegin = RBegin, rend = REnd} = PKRange,
    attrs_to_params(Prefix, [], [{rbegin, RBegin},
				 {rend, REnd},
				 {rtype, Type}]).
    

%%--------------------------------------------------------------------
%% @doc Get paging parameters from paging record list
%% @spec get_paging_params(Prefix :: string(),
%%                     Pagings :: [Paging :: paging()]) ->
%%       [{Param :: string(), Value :: string()} |
%%        {Param :: string(), Value :: binary()}]
%% @end
%%--------------------------------------------------------------------
get_paging_params(Prefix, Pagings) when is_list(Pagings),
					is_record(hd(Pagings), paging) ->
    {_, PagingParams} = lists:foldl(
			  fun(Paging, {N, AccIn}) ->
				  Prefix1 = Prefix ++ "Paging." ++ integer_to_list(N) ++ ".",
				  #paging{name = Name, type = Type, 
					  value = Value} = Paging,
				  {N+1, attrs_to_params(Prefix1, 
						    AccIn,
						    [{name, Name},
						     {value, Value},
						     {type, Type}])}
		      end,
		      {1, []}, Pagings),
    PagingParams.
    
%%--------------------------------------------------------------------
%% @doc Get primary key parameters from primary_key record list
%% @spec get_pk_params(Prefix :: string(),
%%                     PrimaryKeys :: [PrimaryKey :: primary_key()]) ->
%%       [{Param :: string(), Value :: string()} |
%%        {Param :: string(), Value :: binary()}]
%% @end
%%--------------------------------------------------------------------
get_pk_params(Prefix, PrimaryKeys) when is_list(PrimaryKeys),
					is_record(hd(PrimaryKeys), primary_key) ->
    {_, PKParams} = lists:foldl(
		      fun(PK, {N, AccIn}) ->
			      Prefix1 = Prefix ++ "PK." ++ integer_to_list(N) ++ ".",
			      #primary_key{name = Name, type = Type, 
					   value = Value, range = PKRange} = PK,
			      {N+1, attrs_to_params(Prefix1, 
						    AccIn,
						    [{name, Name},
						     {value, Value},
						     {type, Type},
						     {range, PKRange}])}
		      end,
		      {1, []}, PrimaryKeys),
    PKParams.
    

%%--------------------------------------------------------------------
%% @doc Get primary key parameters from primary_key record list
%% @spec get_pk_params(PrimaryKeys :: [PrimaryKey :: primary_key()]) ->
%%       [{Param :: string(), Value :: string()} |
%%        {Param :: string(), Value :: binary()}]
%% @end
%%--------------------------------------------------------------------
get_pk_params(PrimaryKeys)->
    get_pk_params("", PrimaryKeys).

%%--------------------------------------------------------------------
%% @doc Get view parameters from view record list
%% 
%% If view exists, then at least 1 pk is required
%% (如果视图存在，那么至少要提供一个PK列).
%%
%% @spec get_view_params(Prefix :: string(), Views :: [View :: view()]) ->
%%       [{Param :: string(), Value :: string()} | 
%%       {Param :: string(), Value :: binary()}]
%% @end
%%--------------------------------------------------------------------
get_view_params(Prefix, Views) when is_list(Views),
				    is_record(hd(Views), view) ->
    Fun = fun(View, {N, AccIn}) ->
		  Prefix1 = Prefix ++ "View." ++ integer_to_list(N) ++ ".",
		  #view{name = Name, primary_keys = PrimaryKeys, 
			columns = Columns, paging_key_len = PagingKeyLen} = View,
		  {N+1, attrs_to_params(Prefix1, 
					AccIn,
					[{name, Name}, {primary_keys, PrimaryKeys},
					 {columns, Columns}, {paging_key_len, PagingKeyLen}])}
	  end,
    {_, ViewsParams} = lists:foldl(Fun, {1, []}, Views),
    ViewsParams.

sign (Key,Data) ->
    %io:format("StringToSign:~n ~p~n", [Data]),
    binary_to_list( base64:encode( crypto:sha_mac(Key,Data) ) ).

genericRequest(Verb, Params) ->
%%    io:format("Params: ~p~n", [Params]),
    Result = mkReq(Verb, Params),
    case Result of
	{ok, _Status, Body} ->
%%	    io:format("Body:~n~ts~n", [Body]),
	    {ok, Body};
	{error, {_Proto, _Code, _Reason}, Body} ->
	    throw({error, mkErr(Body)})
    end.

get_access_conf(Key) ->
    case ets:lookup(?MODULE, Key) of
	[] ->
	    ErrorMsg = io_lib:format("Can't get the ~p configuration.", [Key]),
	    throw({error, ErrorMsg});
	[{Key, Value}] ->
	    Value
    end.

getProtocol() ->
    "http://".

mkReq(Verb, Params) ->
    Timestamp = lists:flatten(erlaly_util:get_timestamp()),
    QueryParams = [{"SignatureVersion", "1"} |
		   [{"SignatureMethod", "HmacSHA1"} | 
		    [{"OTSAccessKeyId", get_access_conf(ots_access_id)} |
		     [{"Date", Timestamp} |
		      [{"APIVersion", ?API_VERSION} |
		       Params]]]]],
    OrderedParams = lists:reverse(QueryParams),
%%    io:format("OrderedParams:~p~n", [OrderedParams]),
    ParamsString = erlaly_util:mkEnumeration([erlaly_util:url_encode(Key) ++ "=" ++ erlaly_util:url_encode(Value) ||
						 {Key, Value} <- OrderedParams],
					     "&"),
    SignOrderParamsStr = erlaly_util:mkEnumeration([erlaly_util:url_encode(Key) ++ "=" ++ erlaly_util:url_encode(Value) ||
						       {Key, Value} <- lists:keysort(1, QueryParams)],
						   "&"),
    
    StringToSign = "/" ++ Verb ++ "\n" ++ SignOrderParamsStr,
    Signature = sign(get_access_conf(ots_access_key), StringToSign),
    SignatureString = "&Signature=" ++ erlaly_util:url_encode(Signature),
    Url = getProtocol() ++ ?ALY_OTS_HOST ++ "/" ++ Verb ++ "?",
    PostData = ParamsString ++ SignatureString,
%%    io:format("Url:~n~s~n", [Url]),
%%    io:format("PostData:~n~s~n", [PostData]),
    Request = {Url, [], "application/x-www-form-urlencoded", PostData},
    HttpOptions = [{autoredirect, true}],
    Options = [{sync,true}, {body_format, binary}],
    try httpc:request(post, Request, HttpOptions, Options) of
	{ok, {Status, _ReplyHeaders, Body}} ->
	    %% io:format("Body:~p~n", [Body]),
	    case Status of 
		{_, 200, _} -> {ok, Status, binary_to_list(Body)};
		{_, _, _} -> {error, Status, binary_to_list(Body)}
	    end;
	{error, Reason} ->
	    {error, Reason}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.


mkErr(Xml) ->
    {XmlDoc, _Rest} = xmerl_scan:string( Xml ),
    [#xmlText{value=ErrorCode}] = xmerl_xpath:string("//Error/Code/text()", XmlDoc),
    [#xmlText{value=ErrorMessage}] = xmerl_xpath:string("//Error/Message/text()", XmlDoc),
    [#xmlText{value=RequestID}] = xmerl_xpath:string("//RequestID/text()", XmlDoc),
    [#xmlText{value=HostID}] = xmerl_xpath:string("//HostID/text()", XmlDoc),
    {ErrorCode, ErrorMessage, RequestID, HostID}.

attrs_to_params(Prefix, AccIn, Attrs) when is_list(Attrs) ->
    lists:foldl(
      fun(X, Acc) ->
	      case X of
		  {_, undefined} ->
		      Acc;
		  {name, Name} when is_list(Name),
				    is_integer(hd(Name)) ->
		      [{Prefix ++ "Name", Name} | Acc];
		  {group_name, GroupName} when is_list(GroupName),
					       is_integer(hd(GroupName)) ->
		      [{Prefix ++ "GroupName", GroupName} | Acc];
		  {table_name, TableName} when is_list(TableName),
					       is_integer(hd(TableName)) ->
		      [{Prefix ++ "TableName", TableName} | Acc];
		  {checking, Checking} when is_atom(Checking) ->
		      [{Prefix ++ "Checking", get_checking_str(Checking)} | Acc];
		  {rtype, Rtype} when is_atom(Rtype) ->
		      [{Prefix ++ "RangeType", get_type_str(Rtype)} | Acc];
		  {mtype, put} ->
		      [{Prefix ++ "Type", "PUT"} | Acc];
		  {mtype, delete} ->
		      [{Prefix ++ "Type", "DELETE"} | Acc];
		  {type, Type} when is_atom(Type) ->
		      [{Prefix ++ "Type", get_type_str(Type)} | Acc];
		  {value, Value} when is_binary(Value) ->
		      [{Prefix ++ "Value", Value} | Acc];
		  {value, Value} when is_boolean(Value) ->
		      [{Prefix ++ "Value", boolean_to_list(Value)} | Acc];
		  {value, Value} when is_integer(Value) ->
		      [{Prefix ++ "Value", integer_to_list(Value)} | Acc];
		  {value, Value} when is_list(Value),
				      is_integer(hd(Value)) ->
		      [{Prefix ++ "Value", "'" ++ Value ++ "'"} | Acc];
		  {rbegin, inf_min} ->
		      [{Prefix ++ "RangeBegin", "INF_MIN"} | Acc];
		  {rbegin, RBegin} when is_boolean(RBegin) ->
		      [{Prefix ++ "RangeBegin", boolean_to_list(RBegin)} | Acc];
		  {rbegin, RBegin} when is_integer(RBegin) ->
		      [{Prefix ++ "RangeBegin", integer_to_list(RBegin)} | Acc];
		  {rbegin, RBegin} when is_list(RBegin),
				      is_integer(hd(RBegin)) ->
		      [{Prefix ++ "RangeBegin", RBegin} | Acc];
		  {rend, inf_max} ->
		      [{Prefix ++ "RangeEnd", "INF_MAX"} | Acc];
		  {rend, REnd} when is_boolean(REnd) ->
		      [{Prefix ++ "RangeEnd", boolean_to_list(REnd)} | Acc];
		  {rend, REnd} when is_integer(REnd) ->
		      [{Prefix ++ "RangeEnd", integer_to_list(REnd)} | Acc];
		  {rend, REnd} when is_list(REnd),
				      is_integer(hd(REnd)) ->
		      [{Prefix ++ "RangeEnd", REnd} | Acc];
		  {transaction_id, TransactionID} when is_list(TransactionID),
						       is_integer(hd(TransactionID)) ->
		      [{Prefix ++ "TransactionID", TransactionID} | Acc];
		  {primary_keys, PrimaryKeys} when is_list(PrimaryKeys),
						   is_record(hd(PrimaryKeys), primary_key) ->
		      [get_pk_params(Prefix, PrimaryKeys) | Acc];
		  {range, PKRange} when is_record(PKRange, range) ->
		      [get_pk_range_params(Prefix, PKRange) | Acc];
		  {pagings, Pagings} when is_list(Pagings),
					  is_record(hd(Pagings), paging) ->
		      [get_paging_params(Prefix, Pagings) | Acc];
		  {offset, Offset} when is_integer(Offset) ->
		      [{Prefix ++ "Offset", integer_to_list(Offset)} | Acc];
		  {top, Top} when is_integer(Top) ->
		      [{Prefix ++ "Top", integer_to_list(Top)} | Acc];
		  {columns, Columns} when is_list(Columns),
					  is_record(hd(Columns), column) ->
		      [get_column_params(Prefix, Columns) | Acc];
		  {modify, ModifyDataOps} when is_list(ModifyDataOps),
					       is_record(hd(ModifyDataOps), data_op) ->
		      [get_modify_params(Prefix, ModifyDataOps) | Acc];
		  {views, Views} when is_list(Views),
				      is_record(hd(Views), view) ->
		      [get_view_params(Prefix, Views) | Acc];
		  {paging_key_len, PagingKeyLen} when is_integer(PagingKeyLen) ->
		      [{Prefix ++ "PagingKeyLen", integer_to_list(PagingKeyLen)} | Acc]
	      end
      end, AccIn, Attrs).

get_checking_str(no) ->
    "NO";
get_checking_str(insert) ->
    "INSERT";
get_checking_str(update) ->
    "UPDATE".

get_type_str(string) ->
    "STRING";
get_type_str(integer) ->
    "INTEGER";
get_type_str(boolean) ->
    "BOOLEAN";
get_type_str(double) ->
    "DOUBLE".

get_type_atom("STRING") ->
    string;
get_type_atom("INTEGER") ->
    integer;
get_type_atom("BOOLEAN") ->
    boolean;
get_type_atom("DOUBLE") ->
    double.

list_to_boolean("TRUE") ->
    true;
list_to_boolean("FALSE") ->
    false;
list_to_boolean("true") ->
    true;
list_to_boolean("false") ->
    false.

    
boolean_to_list(true) ->
    "TRUE";
boolean_to_list(false) ->
    "FALSE".

    

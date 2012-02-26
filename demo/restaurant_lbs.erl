% -*- coding: utf-8 -*-
%%%-------------------------------------------------------------------
%%% @author Kong Fanbin <kfbuniversity@gmail.com>
%%% @copyright (C) 2012, Kong Fanbin
%%% @doc
%%%
%%% @end
%%% Created : 21 Feb 2012 by Kong Fanbin <kfbuniversity@gmail.com>
%%%-------------------------------------------------------------------
-module(restaurant_lbs).

-include("erlaly_ots.hrl").

-define(RestaurantTable, "RestaurantInfo").
-define(RestaurantCommentTable, "RestaurantComment").
-define(TableGroupName, "CreateTableGroup").

-export([init/2,
	 create_restaurant_info_table/0, 
	 create_restaurant_comment_table/0,
	 insert_restaurant_data/0,
	 query_restaurant_data/0,
	 insert_restaurant_comment/6,
	 query_restaurant_comment/0,
	 test_insert_comment_data/0
	]).

-import(erlaly_util, [guess_type/1]).
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes OTS
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(AccessId, AccessKey) ->
    erlaly:start(),
    ok = erlaly_ots:init(AccessId, AccessKey, false).

create_restaurant_info_table() ->
    %% Table Primary Keys
    %% 表的主键列，其中第一列：DistrictID为数据分片键
    TablePKs = [#primary_key{name = "DistrictID",
			     type = integer},
		#primary_key{name = "RestaurantID",
			     type = integer}],
    Table1 = #table{name = ?RestaurantTable,
		    primary_keys = TablePKs},

    erlaly_ots:create_table(Table1).

insert_restaurant_data() ->
    Restaurants = lists:map(
		    fun(X) ->
			    [{"RestaurantID", X}, {"Name", "饭馆"}, {"Address", "杭州"},
			     {"PhoneNumber", "010-123456789"}, {"Location", "zjfe"}]
		    end, lists:seq(0, 10)),
%%    RestaurantInfo = [{"ID", 1}, {"Name", <<"饭馆">>}, {"Address", <<"杭州">>},
%%		      {"PhoneNumber", <<"123456789">>}, {"Location", <<"zjfe">>}],
    DataPuts = lists:map(
		 fun(X) ->
			 [{"RestaurantID", ID} | OtherInfo] = X,
			 %% Data Primary Keys
			 %% 表的主键列，其中第一列：DistrictID为数据分片键
			 DataPKs = [#primary_key{name = "DistrictID",
						 value = 10000,
						 type = integer},
				    #primary_key{name = "RestaurantID",
						 value = ID,
						 type = guess_type(ID)}],
			 Columns = [#column{name = Name, 
					    value = Value,
					    type = guess_type(Value)} || {Name, Value} <- OtherInfo],
			 #data_op{mtype = put, primary_keys = DataPKs, columns = Columns, checking = no}
		 end, Restaurants),

    {ok, TransactionID, _, _} = erlaly_ots:start_transaction(?RestaurantTable, 10000),
    io:format("TransactionID=~p~n", [TransactionID]),
    Ret = erlaly_ots:batch_modify_data(?RestaurantTable, DataPuts, TransactionID),
    io:format("batch_modify_data Ret=~p~n", [Ret]),
    erlaly_ots:commit_transaction(TransactionID).
    

query_restaurant_data() ->
    %% Data Primary Keys
    %% 表的主键列，其中第一列：DistrictID为数据分片键
    %% 设置range query的前缀和range，RestaurantId值的范围在0-无限大这个区间内
    DataPKs = [#primary_key{name = "DistrictID",
			    type = integer,
			    value = 10000},
	       #primary_key{name = "RestaurantID",
			    range = #range{rtype = integer,
					   rbegin = 0,
					   rend = inf_max}}],

    %% 设置查询返回的columns
    Columns = ["DistrictID","RestaurantID","Name","Address", "PhoneNumber","Location"],
    ColRcds = [#column{name = Name} || Name <- Columns],

    Data1 = #data_op{primary_keys = DataPKs,
		     columns = ColRcds},
    erlaly_ots:get_rows_by_range(?RestaurantTable, Data1).
    
create_restaurant_comment_table() ->
    %% Table Primary Keys
    %% 表的主键列，其中第一列：DistrictID为数据分片键
    %% 设置primary key
    TablePKs = [#primary_key{name = "DistrictID",
			     type = integer},
		#primary_key{name = "RestaurantID",
			     type = integer},
		#primary_key{name = "DateTime",
			     type = string},
		#primary_key{name = "CommentID",
			     type = string}],
    %% 设置paging key的长度为2，也就是设置primary key的前两列为paging key
    Table1 = #table{name = ?RestaurantCommentTable,
		    primary_keys = TablePKs,
		    paging_key_len = 2},

    erlaly_ots:create_table(Table1).

insert_restaurant_comment(DistrictID, RestaurantID, DateTime, UserName, Content, Rate) ->
    Columns = [{"UserName", UserName}, {"Content", Content}, {"Rate", Rate}],
    %% Data Primary Keys
    %% 表的主键列，其中第一列：DistrictID为数据分片键
    DataPKs = [#primary_key{name = "DistrictID",
			    value = DistrictID,
			    type = integer},
	       #primary_key{name = "RestaurantID",
			    value = RestaurantID,
			    type = guess_type(RestaurantID)},
	       #primary_key{name = "DateTime",
			    value = DateTime,
			    type = string},
	       #primary_key{name = "CommentID",
			    value = uuid:v4_string(),
			    type = string}],

    ColRcds = [#column{name = Name, 
		       value = Value,
		       type = guess_type(Value)} || {Name, Value} <- Columns],
    DataPut = #data_op{mtype = put, primary_keys = DataPKs, columns = ColRcds, checking = no},
    io:format("~p comment~n", [UserName]),
    erlaly_ots:put_data(?RestaurantCommentTable, DataPut).

query_restaurant_comment() ->
    %% Data Primary Keys
    %% 表的主键列，其中第一列：DistrictID为数据分片键
    %% 设置range query的前缀和range，RestaurantId值的范围在0-无限大这个区间内
    Pagings = [#paging{name = "DistrictID",
		       type = integer,
		       value = 10000},
	       #paging{name = "RestaurantID",
		       type = integer,
		       value = 8888}],
		       

    %% 设置查询返回的columns
    Columns = ["DistrictID","RestaurantID","DateTime","UserName", "Content","Rate"],
    ColRcds = [#column{name = Name} || Name <- Columns],

    Data1 = #data_op{pagings = Pagings,
		     columns = ColRcds,
		     offset = 50,
		     top = 25
		    },
    erlaly_ots:get_rows_by_offset(?RestaurantCommentTable, Data1).

test_insert_comment_data() ->
    Results = lists:map(
		fun(X) ->
			UserName = "user_" ++ integer_to_list(X),
			%%insert_restaurant_comment(10000, 8888, "2012-02-22", UserName, "test", 5)
			%% 并行插入数据的速度远远快于串行
			spawn(fun() -> insert_restaurant_comment(10000, 8888, "2012-02-22", UserName, "test", 5) end)
		end, lists:seq(0, 10)),

    Errors = lists:filter(
	       fun(R) ->
		       case R of
			   {error, _} ->
			       true;
			   _ ->
			       false
		       end
	       end, Results),
    case Errors of
	[] ->
	    io:format("All comments are inserted~n");
	_ ->
	    io:format("Requests errors:~n~p~n", [Errors])
    end.
%%    RestaurantInfo = [{"ID", 1}, {"Name", <<"饭馆">>}, {"Address", <<"杭州">>},
%%		      {"PhoneNumber", <<"123456789">>}, {"Location", <<"zjfe">>}],

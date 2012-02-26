%%%-------------------------------------------------------------------
%%% @author Kong Fanbin <kfbuniversity@gmail.com>
%%% @copyright (C) 2012, Kong Fanbin
%%% @doc
%%%
%%% @end
%%% Created : 19 Feb 2012 by Kong Fanbin <kfbuniversity@gmail.com>
%%%-------------------------------------------------------------------

% --------------------------------------------------------------------
% Records to represent Aliyun/OTS specific structures.
% --------------------------------------------------------------------

%% ots data type
-type(ots_partition_key_t() :: string | integer).

-type(ots_pk_data_t() :: string | integer | boolean).

-type(ots_name_t() :: string()).

-type(ots_value_t() :: string()).

-type(ots_column_data_t() :: string | integer | boolean | double).

-type(checking_t() :: no | insert | update).
-type(data_op_t() :: put | delete).

-record(range, {
	  rtype      :: ots_pk_data_t(), 
	  rbegin     :: ots_value_t(), 
	  rend       :: ots_value_t()
	 }).

-type(range() :: #range{}).

%% 按range查询时，最后一个PK列必须设定为range
%% primary_key record
-record(primary_key, {
	  name            :: ots_name_t(),
	  type            :: ots_pk_data_t(),
	  value           :: ots_value_t(),
	  range           :: range()
	  }).
-type(primary_key() :: #primary_key{}).

%% paging record
-record(paging, {
	  name            :: ots_name_t(),
	  type            :: ots_pk_data_t(),
	  value           :: ots_value_t()
	  }).
-type(paging() :: #paging{}).

%% column record
-record(column, {
	  is_pk           :: boolean(),
	  name            :: ots_name_t(),
	  type            :: ots_column_data_t(),
	  value           :: ots_value_t()
	  }).

-type(column() :: #column{}).

%% view record
-record(view, {
	  name            :: ots_name_t(),
	  primary_keys    :: [primary_key()],
	  columns         :: [column()], 
	  paging_key_len  :: integer()
	  }).

-type(view() :: #view{}).

%% table record
-record(table, {
	  name            :: string(),
	  primary_keys    :: [primary_key()],
	  paging_key_len  :: integer(),
	  views           :: [view],
	  group_name      :: ots_name_t()
	 }). 
	  
-type(table() :: #table{}).
	  
%% data_op record
-record(data_op, {
	  %% only required for batch_modify_data
	  mtype           :: data_op_t(),

	  primary_keys    :: [primary_key()],
	  columns         :: [column()],
	  %% only required for get_rows_by_offset
	  pagings         :: [paging()],
	  %% only optional for put operation
	  checking = no   :: checking_t(),
	  %% only required by get_rows_by_offset
	  offset = 0      :: integer(),
	  %% required for get_rows_by_offset
	  %% and optional for get_rows_by_range
	  top             :: integer()
	 }). 
-type(data_op() :: #data_op{}).
	  

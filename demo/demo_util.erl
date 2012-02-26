%%%-------------------------------------------------------------------
%%% @author Kong Fanbin <kfbuniversity@gmail.com>
%%% @copyright (C) 2012, Kong Fanbin
%%% @doc
%%%
%%% @end
%%% Created : 23 Feb 2012 by Kong Fanbin <kfbuniversity@gmail.com>
%%%-------------------------------------------------------------------
-module(demo_util).

-include("erlaly_ots.hrl").

-import(erlaly_util, [guess_type/1]).
-export([generate_col_rcds/2, generate_data_op/2,
	get_row_data_op/1, get_row_data_op/2,
	get_row_by_range_data_op/1, get_row_by_range_data_op/2,
	get_row_by_offset_data_op/2, get_row_by_offset_data_op/3
	]).


%% 由行的每一列的属性和行的每一列的数据产生一行column记录
-spec generate_col_rcds(ColAttrs :: [{boolean(), string()}], RowData :: [Columns :: string()]) ->
			       [ColRcd :: column()].

generate_col_rcds(ColAttrs, RowData) when is_list(ColAttrs),
					  is_list(RowData) ->
    lists:zipwith(
      fun({IsPK, Name}, Value) ->
	      #column{name = Name, is_pk = IsPK,
		      value = Value, type = guess_type(Value)}
      end, ColAttrs, RowData).

%% 由一行中所有列的column记录产生出一个data_op记录
generate_data_op(RowColRcds, Op) when is_list(RowColRcds),
				       is_record(hd(RowColRcds), column) ->
    lists:foldl(fun(#column{is_pk = IsPK,
			    name = Name,
			    value = Value,
			    type = Type},
		    #data_op{primary_keys = PrimaryKeys,
			     columns = Columns}) ->
			case IsPK of
			    true ->
				NewPrimaryKeys = [#primary_key{name = Name,
							       value = Value,
							       type = Type} | PrimaryKeys],
				NewColumns = Columns;
			    false ->
				NewColumns = [#column{name = Name,
						      value = Value,
						      type = Type} | Columns],
				NewPrimaryKeys = PrimaryKeys
			end,
			case Op of
			    delete ->
				#data_op{mtype = Op, 
					 primary_keys = NewPrimaryKeys,
					 columns = undefined};
			    put ->
				#data_op{mtype = Op, 
					 primary_keys = NewPrimaryKeys,
					 columns = NewColumns}
			end
		end,
		#data_op{primary_keys = [], columns = []}, 
		lists:reverse(RowColRcds)).
			

%% 由一行中所有列的column记录产生出一个data_op记录
generate_data_op2(RowColRcds, Op) when is_list(RowColRcds),
				       is_record(hd(RowColRcds), column) ->
    DataOp = lists:foldl(
	       fun(#column{is_pk = IsPK,
			   name = Name,
			   value = Value,
			   type = Type},
		   #data_op{primary_keys = PrimaryKeys} = CurDataOp) ->
		       case IsPK of
			   true ->
			       CurDataOp#data_op{primary_keys = [#primary_key{name = Name,
									      value = Value,
									      type = Type} | PrimaryKeys]};
			   false ->
			       CurDataOp
		       end
	       end,
	       #data_op{primary_keys = []}, 
	       lists:reverse(RowColRcds)),
    case Op of
	delete ->
	    DataOp#data_op{mtype = Op, columns = undefined};
	put ->
	    DataOp#data_op{mtype = Op}
    end.

%% 由一行中所有列的column记录产生出一个用于get_row 操作的 data_op 记录
%% 可以指定要获取的列名称，不指定则默认获取所有列
get_row_data_op(RowColRcds) when is_list(RowColRcds),
				 is_record(hd(RowColRcds), column) ->
    get_row_data_op(RowColRcds, all).

get_row_data_op(RowColRcds, Cols) when is_list(RowColRcds),
				       is_record(hd(RowColRcds), column) ->
    DataOp = lists:foldl(
	       fun(#column{is_pk = IsPK,
			   name = Name,
			   value = Value,
			   type = Type},
		   #data_op{primary_keys = PrimaryKeys} = CurDataOp) ->
		       case IsPK of
			   true ->
			       CurDataOp#data_op{primary_keys = [#primary_key{name = Name,
									      value = Value,
									      type = Type} | PrimaryKeys]};
			   false ->
			       CurDataOp
		       end
	       end,
	       #data_op{primary_keys = []}, 
	       lists:reverse(RowColRcds)),

    case Cols of
	all ->
	    DataOp;
	_ when is_list(Cols),
	       is_list(hd(Cols)),
	       is_integer(hd(hd(Cols)))->
	    DataOp#data_op{columns = [#column{name = C} || C <- Cols]}
    end.


			
%% 由一行中所有列的column记录产生出一个用于get_row_by_range 操作的 data_op 记录
%% 可以指定要获取的列名称，不指定则默认获取所有列
%% 最后一个PK列的将作为 Range 参数，其取值范围默认为 inf_min ~ inf_max，即无穷小至无穷大
get_row_by_range_data_op(RowColRcds) when is_list(RowColRcds),
					  is_record(hd(RowColRcds), column) ->
    get_row_by_range_data_op(RowColRcds, all).

get_row_by_range_data_op(RowColRcds, Cols) when is_list(RowColRcds),
						is_record(hd(RowColRcds), column) ->
    DataOp = lists:foldl(
	       fun(#column{is_pk = IsPK,
			   name = Name,
			   value = Value,
			   type = Type},
		   #data_op{primary_keys = PrimaryKeys} = CurDataOp) ->
		       case IsPK of
			   true ->
			       CurDataOp#data_op{primary_keys = [#primary_key{name = Name,
									      value = Value,
									      type = Type} | PrimaryKeys]};
			   false ->
			       CurDataOp
		       end
	       end,
	       #data_op{primary_keys = []}, 
	       RowColRcds),
    #data_op{primary_keys = ReversePKs} = DataOp,
    [#primary_key{type = Type, name = Name} | OtherPKs ] = ReversePKs,
    RangingPK = #primary_key{name = Name,
			     range = #range{rtype = Type,
					    rbegin = inf_min,
					    rend = inf_max}},
    NewDataOp = DataOp#data_op{primary_keys = lists:reverse([RangingPK | OtherPKs])},

    case Cols of
	all ->
	    NewDataOp;
	_ when is_list(Cols),
	       is_list(hd(Cols)),
	       is_integer(hd(hd(Cols)))->
	    NewDataOp#data_op{columns = [#column{name = C} || C <- Cols]}
    end.

%% 由参数中提供的 column 记录产生出一个用于 get_row_by_offset 操作的 data_op 记录
%% 可以指定要获取的列名称，不指定则默认获取所有列
%% 最后一个PK列的将作为 Offset 参数，其取值范围默认为 inf_min ~ inf_max，即无穷小至无穷大
get_row_by_offset_data_op(ColRcds, {Offset, Top}) when is_list(ColRcds),
						       is_record(hd(ColRcds), column),
						       is_integer(Offset),
						       is_integer(Top) ->
    get_row_by_offset_data_op(ColRcds, {Offset, Top}, all).

get_row_by_offset_data_op(ColRcds, {Offset, Top}, Cols) when is_list(ColRcds),
							     is_record(hd(ColRcds), column),
							     is_integer(Offset),
							     is_integer(Top) ->
    DataOp = lists:foldl(
	       fun(#column{name = Name,
			   value = Value,
			   type = Type},
		   #data_op{pagings = Pagings} = CurDataOp) ->
		       CurDataOp#data_op{pagings = [#paging{name = Name,
							    value = Value,
							    type = Type} | Pagings]}
	       end,
	       #data_op{pagings = []}, 
	       lists:reverse(ColRcds)),
    NewDataOp = DataOp#data_op{offset = Offset, top = Top},
    case Cols of
	all ->
	    NewDataOp;
	_ when is_list(Cols),
	       is_list(hd(Cols)),
	       is_integer(hd(hd(Cols)))->
	    NewDataOp#data_op{columns = [#column{name = C} || C <- Cols]}
    end.


			

			

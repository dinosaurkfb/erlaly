-module(erlaly_util).

-export([url_encode/1, mkEnumeration/2, queryParams/1, get_timestamp/0, guess_type/1]).

guess_type(Value) when is_list(Value),
		       is_integer(hd(Value)) ->
    string;
guess_type(Value) when is_integer(Value) ->
    integer;
guess_type(Value) when is_float(Value) ->
    double;
guess_type(Value) when is_boolean(Value) ->
    boolean.
    
get_timestamp() ->
    UTime = erlang:universaltime(),
%%    aly_fmt(erlang:universaltime_to_localtime(UTime)).
    aly_fmt(UTime).

month_to_str(Month) ->
    element(Month, {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}).

date_to_dw_str(Date) ->
    DW = calendar:day_of_the_week(Date),
    element(DW, {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}).

aly_fmt(DateTime) ->
    {Date,{Hour,Min,Sec}} = DateTime,
    {Year,Month,Day} = Date,
    MonStr = month_to_str(Month),
    DWStr = date_to_dw_str(Date),
    io_lib:format("~s ~s ~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B ~4.10.0B",
		  [DWStr, MonStr, Day, Hour, Min, Sec, Year]).

mkEnumeration(Values, Separator) ->
    lists:flatten(string:join(Values, Separator)).

queryParams( [] ) -> "";
queryParams( ParamList ) -> 
    L0 = lists:foldl(fun({P, V}, Acc) ->
			     case V of
				 "" ->
				     [url_encode(P)|Acc];
				 _ ->
				     [url_encode(P) ++ "=" ++ url_encode(V)|Acc]
			     end
		     end,
		     [],
		     lists:keysort(1, ParamList)),
    "?" ++ mkEnumeration(lists:reverse(L0), "&"). 

%% The following code is taken from the ibrowse Http client
%% library.
%%
%% Original license:
%%
%% Copyright (c) 2006, Chandrashekhar Mullaparthi
%% 
%% Redistribution and use in source and binary forms, with or without 
%% modification, are permitted provided that the following conditions are met:
%%
%%     * Redistributions of source code must retain the above copyright notice,
%%       this list of conditions and the following disclaimer.
%%     * Redistributions in binary form must reproduce the above copyright
%%       notice, this list of conditions and the following disclaimer in the 
%%       documentation and/or other materials provided with the distribution.
%%     * Neither the name of the T-Mobile nor the names of its contributors 
%%       may be used to endorse or promote products derived from this software
%%       without specific prior written permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
%% ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
%% LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
%% CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
%% SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
%% INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
%% CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
%% ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
%% THE POSSIBILITY OF SUCH DAMAGE.


url_encode(Str) when is_list(Str) ->
    url_encode_char(lists:reverse(Str), []).

url_encode_char([X | T], Acc) when X >= $0, X =< $9 ->
    url_encode_char(T, [X | Acc]);
url_encode_char([X | T], Acc) when X >= $a, X =< $z ->
    url_encode_char(T, [X | Acc]);
url_encode_char([X | T], Acc) when X >= $A, X =< $Z ->
    url_encode_char(T, [X | Acc]);
url_encode_char([X | T], Acc) when X == $-; X == $_; X == $. ->
    url_encode_char(T, [X | Acc]);
% Commented out to strictly comply with ALY requirement -- goura
%url_encode_char([32 | T], Acc) ->
%    url_encode_char(T, [$+ | Acc]);
url_encode_char([X | T], Acc) ->
    url_encode_char(T, [$%, d2h(X bsr 4), d2h(X band 16#0f) | Acc]);
			url_encode_char([], Acc) ->
			       Acc.

d2h(N) when N<10 -> N+$0;
% Commented out to strictly comply with ALY requirement -- goura
%d2h(N) -> N+$a-10.
d2h(N) -> N+$A-10.

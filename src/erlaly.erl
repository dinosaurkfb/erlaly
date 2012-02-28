%%%-------------------------------------------------------------------
%%% @author Kong Fanbin <kfbuniversity@gmail.com>
%%% @copyright (C) 2012, Kong Fanbin
%%% @doc This is an client implementation for Aliyun's OTS WebService
%% 
%%% @end
%%% Created : 16 Feb 2012 by Kong Fanbin <kfbuniversity@gmail.com>
%%%-------------------------------------------------------------------
-module(erlaly). 

-behaviour(application). 

-export([start/0, start/2, stop/1]). 

start() ->
    crypto:start(),
    inets:start().

start(_Type, _Args) -> 
	erlaly:start().
	
stop(_State) -> 
    ok. 

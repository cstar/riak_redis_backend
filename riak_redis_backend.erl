%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at

%%   http://www.apache.org/licenses/LICENSE-2.0
%%[{ totals,                                     4239,   44.484,   22.362}].
%%[{ totals,                                     4951,   36.381,   26.965}].


%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.    

%% @doc riak_redis_backend is a Riak storage backend using erldis.


%% Blog post about the initial release : 
%% http://cestari.wordpress.com/2009/12/22/a-redis-backend-for-riak/
-module(riak_redis_backend).
-author('Eric Cestari <eric@ohmforce.com').
-export([start/1,stop/1,get/2,put/3,list/1,list_bucket/2,delete/2]).

% @type state() = term().
-record(state, {pid}).

% @spec start(Partition :: integer()) ->
%                        {ok, state()} | {{error, Reason :: term()}, state()}
start(_Partition)->
  {ok, Pid} = erldis_sync_client:connect("localhost"),
  erldis:select(Pid, erlang:phash2(node(), 40)),
  {ok, #state{pid=Pid}}.

% @spec stop(state()) -> ok | {error, Reason :: term()}  
stop(_State)->
  ok.

% get(state(), Key :: binary()) ->
%   {ok, Val :: binary()} | {error, Reason :: term()}
get(#state{pid=Pid}, BK)->
  case erldis:get(Pid, k2l(BK)) of
    nil -> {error, notfound};
    Val -> {ok, binary_to_term(Val)}
  end.

% put(state(), Key :: binary(), Val :: binary()) ->
%   ok | {error, Reason :: term()}  
put(#state{pid=Pid}, {Bucket, Key}=BK, Value)->
  %riak_eventer:notify(riak_redis_backend, put, {{Bucket, Key}, Value}),
  erldis:sadd(Pid, <<"buckets">>,Bucket),
  case erldis:set(Pid, k2l(BK), term_to_binary(Value)) of
    ok -> 
      erldis:sadd(Pid, Bucket, Key),
      erldis:sadd(Pid, <<"world">>, term_to_binary(BK)),
      ok;
    _ -> {error, unable_to_put}
  end.

% delete(state(), Key :: binary()) ->
%   ok | {error, Reason :: term()}
delete(#state { pid=Pid }, {Bucket, Key}=BK) ->
  case erldis:del(Pid, k2l(BK)) of
    true -> 
      erldis:srem(Pid, Bucket, Key),
      erldis:srem(Pid, <<"world">>, term_to_binary(BK)),
      ok;
    _ -> 
      {error, unable_to_delete}
  end.
  
% list(state()) -> [Key :: binary()]
list(#state { pid=Pid }) ->
  lists:map(fun binary_to_term/1, 
      erldis:smembers(Pid, <<"world">>)).

list_bucket(#state { pid=Pid }, '_')->
  erldis:smembers(Pid, <<"buckets">>);  
    
list_bucket(#state { pid=Pid }, {filter, Bucket, Fun})->
  lists:filter(Fun, erldis:smembers(Pid, Bucket));
list_bucket(#state { pid=Pid }, Bucket) ->
  erldis:smembers(Pid, Bucket).



k2l({B, V})->
  <<B/binary,V/binary>>.
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
    Val -> {ok, l2t(Val)}
  end.

% put(state(), Key :: binary(), Val :: binary()) ->
%   ok | {error, Reason :: term()}  
put(#state{pid=Pid}, {Bucket, Key}=BK, Value)->
  %riak_eventer:notify(riak_redis_backend, put, {{Bucket, Key}, Value}),
  check_bucket(Pid, Bucket),
  case erldis:set(Pid, k2l(BK), t2l(Value)) of
    ok -> 
     case {erldis:sismember(Pid, binary_to_list(Bucket), t2l(Key)), 
           erldis:sismember(Pid, "world", t2l(BK))} of
       {false, false} ->
         erldis:sadd(Pid, binary_to_list(Bucket), t2l(Key)),
         erldis:sadd(Pid, "world", t2l(BK)),
         ok;
        _ ->
          ok
      end;
    _ -> {error, unable_to_put}
  end.

% delete(state(), Key :: binary()) ->
%   ok | {error, Reason :: term()}
delete(#state { pid=Pid }, {Bucket, Key}=BK) ->
  case erldis:del(Pid, k2l(BK)) of
    true -> 
      case {erldis:sismember(Pid, binary_to_list(Bucket), t2l(Key)), 
            erldis:sismember(Pid, "world", t2l(BK))} of
       {true, true} ->
         erldis:srem(Pid, binary_to_list(Bucket), t2l(Key)),
         erldis:srem(Pid, "world", t2l(BK)),
         ok;
        _ ->
          ok
      end;
    _ -> {error, unable_to_delete}
  end.
  
% list(state()) -> [Key :: binary()]
list(#state { pid=Pid }) ->
  lists:map(fun(Key)->
    l2t(Key)
  end,
  erldis:smembers(Pid, "world")).

list_bucket(#state { pid=Pid }, '_')->
  lists:map(fun(Key)->
    l2t(Key)
  end,
  erldis:smembers(Pid, "buckets"));  
    
list_bucket(#state { pid=Pid }, {filter, Bucket, Fun})->
  KL = lists:filter(Fun, erldis:smembers(Pid, binary_to_list(Bucket))),
  lists:map(fun(Key)->
    l2t(Key)
  end, KL);
list_bucket(#state { pid=Pid }, Bucket) ->
  lists:map(fun(Key)->
    l2t(Key)
  end,
  erldis:smembers(Pid, binary_to_list(Bucket))).

check_bucket(Pid,Bucket)->
  B = t2l(Bucket),
  case erldis:sismember(Pid, "buckets", B) of
    true ->
      ok;
    false ->
      erldis:sadd(Pid, "buckets",B)
  end.


k2l({B, V})->
  binary_to_list(list_to_binary([B, V])).
t2l(V)->
  binary_to_list(term_to_binary(V)).
  
l2t(V)->
  binary_to_term(list_to_binary(V)).
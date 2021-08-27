# Introduction
Assignments of MIT [6.824](https://pdos.csail.mit.edu/6.824/schedule.html).

# Raft 

## 实现Raft几个关键点

1. RPC的实现中要先判断term是否为old，对于old term直接拒绝。
2. 处理RPC响应时要先判断自己的term是否old，如果是则转为follower状态，不作后续处理；然后再判断响应是
   否过期的，即自从发请求以来状态是否有变化（比如term、nextIndex是否发生变化），如果有则忽略这个响应。
3. 每个RPC请求应该是异步的，防止某个RPC过慢阻塞其它的RPC.
4. 实现linearizable读有两种方法：
   1. 与写操作类似，收到读请求后先提交no-op操作，获得commit index（保证是最新的，一定比之前的写的
      index大），等待apply完成后再读；
   2. (lease read) 不用走提交协议，可直接读。刚当选leader时先提交no-op操作（因为刚刚当选的leader不一定有最新的
      commit index），完成后再对外支持读操作，并保证leader在一段时间不能变（比如10s），在这段时间内
      可以直接支持读操作（要等当前的commit index被apply），不用每次都要提交no-op操作。但是集群要保证
      各自节点的时钟是高度同步的。
5. 实现linearizable写时要注意，propose的操作不一定会成功提交，因为当前leader可能会退化为follower，刚
   刚propose的index可能被别的操作替代，所以等到此index被apply后还需要检查term是否匹配，如果不匹配就
   说明没提交成功，需要重试。

## 优化点

1. AppendEntries的响应指出log不匹配时主节点需要调整nextIndex的估计，这里可以优化成nextIndex往后退一
   个term，而不是每次只退一个entry. 不过在测试时这种优化效果并不明显。
2. 在RPC带宽消耗与提交延迟之间权衡。Raft每次提交一条entry需要两轮交互。如果应用每次提交一条entry都要
   Raft立即开始提交流程，可以加快提交数据的速度，但是会增加Raft的RPC次数，每次AppendEntries请求只能
   携带一条数据。如果让数据随着每次主节点的心跳来提交数据，可以节省RPC次数，并且可以一次
   AppendEntries携带两次心跳之间应用提交的数据，只是提交数据的延迟会变大。
3. 提交entry的第二轮交互可以立即开始，不用等待下次心跳触发。处理AppendEntries的响应时如果发现
   commitIndex增加了可以立即触发提交的第二轮交互，可以大大加快提交的速度。
4. Leader与每个follower的心跳可以按每个follower的进度独立进行，不需要按统一步伐。
5. 凡是需要提供linearizable的操作都需要经过Raft，不能直接修改状态机的状态。例如，linearizable读就需
   要提交Raft执行，不能直接读状态机。
   
# ShardKV

1. 实现KV时要分清哪些是持久化状态（影响查询结果且重启后不能重建的信息），持久化状态要记录进
   snapshot. 比如ShardKV的持久化状态包括每个shard的kv、操作去重的状态以及shard的分配config。
2. 在转移Shard的过程中不要变更config，会导致中间状态过多，实现困难，而且也会引起多余的操作（比如一个
   shard一开始是给G1，转移还没完成又要转移给G2，导致重复转移）。转移时要处理重复的转移请求（可能有失
   败而重试）。
3. 转移shard时要保证以下两点：
   1. 任何时候只有一个group处理这个shard，在处理请求时要求客户端与服务端的config保持一致可解决这个问题。
   2. 任何时候shard的owner只有一个。依次按顺序变更config，当shard转移完成（包括把不需要的shard转移出
      去和等待别的group转移shard给我）之后才变更到下一个config。
4. 执行提交的operation时要保证算法是确定性的，否则会出现各节点的状态不一致。比如`slice.Sort()`就不一定是确定性的。

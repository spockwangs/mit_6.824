# mit_6.824
Assignments of MIT [6.824](https://pdos.csail.mit.edu/6.824/schedule.html).

# Raft 

## 实现Raft几个关键点

1. RPC的实现中要先判断term是否为old，对于old term直接拒绝。
2. 处理RPC响应时要先判断自己的term是否old，如果是则转为follower状态，不作后续处理；然后再判断响应是
   否过期的，即自从发请求以来状态是否有变化（比如term、nextIndex是否发生变化），如果有则忽略这个响应。
3. 每个RPC请求应该是异步的，防止某个RPC过慢阻塞其它的RPC.

## 优化点

1. AppendEntries的响应指出log不匹配时主节点需要调整nextIndex的估计，这里可以优化成nextIndex往后退一
   个term，而不是每次只退一个entry. 不过在测试时这种优化效果并不明显。
2. 在RPC带宽消耗与提交延迟之间权衡。Raft每次提交一条entry需要两轮交互。如果应用每次提交一条entry都要
   Raft立即开始提交流程，可以加快提交数据的速度，但是会增加Raft的RPC次数，每次AppendEntries请求只能
   携带一条数据。如果让数据随着每次主节点的心跳来提交数据，可以节省RPC次数，并且可以一次
   AppendEntries携带两次心跳之间应用提交的数据，只是提交数据的延迟会变大。
3. 提交entry的第二轮交互可以立即开始，不用等待下次心跳触发。处理AppendEntries的响应时如果发现
   commitIndex增加了可以立即触发提交的第二轮交互，可以大大加快提交的速度。
   

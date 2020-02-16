# mit_6.824
Assignments of MIT [6.824](https://pdos.csail.mit.edu/6.824/schedule.html).

# Raft 

## 实现Raft几个关键点

1. RPC的实现中要先判断term是否为old，对于old term直接拒绝。
2. 处理RPC响应时要先判断自己的term是否old，如果是则转为follower状态，不作后续处理；然后再判断响应是
   否过期的，即自从发请求以来状态是否有变化（比如term、nextIndex是否发生变化），如果有则忽略这个响应。
3. 每个RPC请求应该是异步的，防止某个RPC过慢阻塞其它的RPC.

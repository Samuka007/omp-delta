# omp-delta

把 oh-my-pi(omp)会话实时记录进 Delta 线程的 omp 扩展——按 [Delta 插件合约 v8](https://delta.dev/docs/cli/build-a-plugin) 实现的社区版 CLI 插件(官方 Pi 插件尚未发布)。

## 它做什么

- `/delta-share` — 把当前会话连接到一条 Delta 线程:建线程(幂等)→ 挂载 worktree(含未提交改动)→ 共享给组织(先弹确认)
- 之后自动记录:用户输入、turn 起止、助手回复、工具调用,推送到 Delta(在 Delta 应用/浏览器里可实时围观、评论)
- 队友在 Delta 里的评论会作为 **follow-up** 回流进 omp 上下文(不会自动触发 turn)
- 线程被他人接管时自动停止本地 turn(superseded / fenced)
- 断线自动重试;本地 outbox 保证记录不丢(Delta 未确认前不删,重发安全)
- `/delta-status` `/delta-detach` `/delta-retry` 管理连接

## 安装

```sh
cp omp-delta.ts ~/.omp/agent/extensions/omp-delta.ts
```

前提:Delta 桌面版 ≥ 0.23.0(含 delta CLI)并已登录(`delta auth status`)。

## 已知边界(MVP)

- 只记录主会话;omp 子代理 → Delta 子线程(`thread create --parent`)尚未实现
- 不回导连接前的历史消息
- 思考块(thinking)、流式增量不记录;工具卡只有名称与状态,无参数/输出正文
- 会话切换(/new 等)会断开同步,需在新会话重新 /delta-share
- `thread share` 固定为 organization 可见性
- 共享目标为当前选中的组织（delta org current）；如需切换先 delta org switch
- 必须在 git 仓库目录里连接（worktree 挂载需要）(连接前有确认对话框)

## 验证过的部分

- `node --check`(strip-types)语法通过、模块可加载
- delta CLI 0.23.0 合约返回 `{min:8,max:8}`,与插件匹配

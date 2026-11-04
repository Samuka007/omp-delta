# omp-delta

omp 扩展:把 oh-my-pi (omp) 会话录制进 Delta 线程(`/delta-share`、实时 watch、评论回流)。架构与合约细节见 `docs/contract-notes.md`;路线图以 GitHub issues 为单一真相源(#10 起:零本地状态,身份/游标全部取自服务端)。

## 环境(由 Delta `.agents/prepare` 自动执行,手动时如下)

```sh
bun install        # 依赖由 bun.lock 锁定
bun run typecheck  # tsc --noEmit
bun test           # tests/ 下 42 个用例
```

## 约定

- 改 `omp-delta.ts` 后必须跑 `bun run typecheck` + `bun test`;两者都绿才算完成。
- Windows 注意事项(Git Bash 路径、进程/管道怪癖)见 `docs/contract-notes.md` 末尾。
- 提交信息用英文祈使句,正文可中文。

## 分支与落地(硬性规则)

- **commit 允许,push 禁止**:agent/subagent 在自己的 worktree 内正常 commit(提交信息引用对应 issue `(#N)`);任何情况下不得 `git push`、不得 merge 到 main、不得建远程分支。push 是用户的显式动作。
- **变更汇聚走 Delta**:子线程的变更由父线程 `merge_worktrees` 拉取;落地(推送 origin/main)只通过 **Land Changes** 流程触发 `.agents/skills/land`,由用户点击按钮授权。
- **issue 回收属于落地**:land skill 在推送后按 commit 中的 issue 引用逐项关闭并附证据;被解除阻塞的 issue(`Blocked by` 关系)留评论更新状态。

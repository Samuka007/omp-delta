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

---
name: land
description: 落地本仓库变更到 origin/main:跑检查、finalize 提交、推送、关闭对应 issue。由 Delta Land Changes 按钮或用户明确指令触发;agent 不得在未授权时调用。
metadata:
  delta-action: land
---

# Land changes to origin/main

本仓库的落地流程。前置:所有要落地的变更已在当前 worktree 中 commit(或明确属于本次落地的一部分)。

## 1. 检查(不绿不落地)

```sh
bun run typecheck && bun test
```

任一失败:停止,报告失败原因,不推送。

## 2. Finalize 提交

- 未提交的残余变更:确认属于本次落地 → 补一笔 `chore: land remaining changes (...)`;不属于 → 列出并询问。
- 提交信息:英文祈使句标题,引用对应 issue `(#N)`;正文可中文。
- 确认没有 secret、临时文件、`probe-out.txt` 之类诊断产物混入。
- 有多个 commit 时保持原样(审查需要粒度),不要 squash。

## 3. 推送

```sh
git push origin HEAD:main
```

只在第 1 步全绿后执行。推送被拒(远端有新提交)时:rebase 后重跑检查再推,不 force-push。

## 4. Issue 回收

对本笔落地涉及的每个 issue:

1. commit 已引用 `#N` → `gh issue close N -c "<证据>"`:附 commit hash、验证结果(typecheck/test 输出摘要)、以及为何满足验收标准。
2. 验收只有部分满足 → 不关闭,留进度评论说明剩余项。
3. 被本落地解除阻塞的 issue(其 `Blocked by` 指向已关闭项)→ 留评论标记可开工,不改标签。
4. 更新 thread 里的 `ready-for-agent` 标签状态:已完成的从 open 列表移除(关闭即可)。

## 5. 汇报

最后输出:推送的 commit 范围(hash 区间)、检查结果、关闭/评论的 issue 清单。若在 Delta Land Changes 流程内,按 `report-landed` builtin skill 的格式汇报。

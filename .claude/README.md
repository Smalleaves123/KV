# `.claude` 协作入口

这个目录用于定义本仓库的 AI 协作规范。目标不是“写得好看”，而是让后续辅助工作稳定、可验证、可回滚。

## 推荐入口

- `kv-helper`：默认项目协作模式，适用于排查 bug、实现功能、补测试、做提交前验证。
- `old-code`：深度工程模式，适用于需要白盒分析、原理推导、边界审计、复杂系统排查的任务。

## 模块入口

- [.claude/MODULES.md](/Users/aiziqi/Desktop/KV_/.claude/MODULES.md)：仓库子系统地图和检查清单。
- [.claude/skills/kv-helper/SKILL.md](/Users/aiziqi/Desktop/KV_/.claude/skills/kv-helper/SKILL.md)：默认协作规范。
- [.claude/skills/old-code/SKILL.md](/Users/aiziqi/Desktop/KV_/.claude/skills/old-code/SKILL.md)：深度工程模式。

## 工作原则

- 先理解再修改。
- 先最小复现，再最小修复。
- 先补回归测试，再扩大改动。
- 先验证正确性，再谈性能优化。
- 任何涉及持久化、并发、协议、Raft、事务的变更，都要默认高风险处理。

## 交付标准

- 代码改动应尽量小而直接。
- 修改后要说明影响范围。
- 要给出已跑过的验证命令和结果。
- 如果还有残余风险，要明确写出来。

## 目录说明

- `commands/`：可触发的命令入口。
- `skills/`：更完整的协作规则和工作流。
- `settings.local.json`：本地权限与工具允许列表。
- `MODULES.md`：按模块分解的检查点和验证策略。

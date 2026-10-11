<h1 align="center">
  <img src="./src-tauri/icons/icon.png" alt="Flow Clash" width="128" />
  <br>
  Flow Clash 1.0.0
</h1>

<h3 align="center">
  基于 Clash Verge Rev 的智能寻路桌面代理
</h3>

<p align="center">
  上游项目：<a href="https://github.com/clash-verge-rev/clash-verge-rev">Clash Verge Rev v2.5.8</a> · 许可证继承 <a href="./LICENSE">GPL-3.0</a>
</p>

---

## 这是什么

Flow Clash 是 [Clash Verge Rev](https://github.com/clash-verge-rev/clash-verge-rev) 的本地改造版。
上游提供了优秀的 Tauri + React 桌面外壳与 Mihomo 内核管理；本项目在其之上重写了线路选择层，
通过**智能寻路**保证日常流量与 AI 服务（OpenAI / Claude / Gemini）使用的稳定性。

## 智能寻路做了什么

- **订阅角色分工**：右键订阅卡片设为前置/后置。非 AI 流量在两个订阅的节点中自动选择最优单跳；
  AI 流量专走「前置 → 后置」两跳链路，后置订阅的住宅节点只服务 AI。
- **启动即连通**：启动时先恢复上次可用线路，验证通过才启用系统代理，寻路在后台进行，用户不用干等。
- **分级验证与淘汰**：每条候选都要通过实际代理端口的端点验证（AI 检查 API/网页/开发工具三类端点）；
  Cloudflare 验证页与硬封锁分开判定、按证据累积淘汰，只有真实可用的线路会被切入和保持。
- **故障自动恢复**：连续失败才确认故障，分批扩大搜索、失败候选冷却、有界退避；健康 AI 出口保持稳定，
  确认故障后优先保留后置、更换前置。
- **网络与出口安全**：网络变化防抖后复验并保留原出口；运行时出口探测识别真实出口 IP 与国家，
  中国大陆出口三重防线拦截（节点识别、强制地区排除、出口实测），AI 跟随不进入排除地区。
- **状态透明**：实时显示各分组当前线路、实际出口 IP/国家、探测进度、下一次检查与恢复时间。

## 与上游的主要差异

| 领域 | 说明 |
| --- | --- |
| 独立身份 | `dev.flowclash.rev`，与 Clash Verge Rev 可并存；数据目录、服务、深层链接全部独立 |
| 后台服务 | 随源码附带改造版服务（`vendor/`），服务名 `flowclash_service` |
| 端口 | 默认混合端口 17970、控制端口 19097 |
| 更新 | 停用上游应用内更新，跟随本仓库演进 |
| 路由层 | 新增 `src-tauri/src/core/routing/`（引擎/目录/探测）与设置页「链式与 AI 线路」 |

上游的订阅管理、规则、连接查看、Mihomo 内核管理等能力全部保留，感谢上游贡献者。

## 构建

- macOS（Apple Silicon）：`node scripts/package-macos.mjs`
- Windows x64：参见 `WINDOWS_BUILD.md`
- 常规 Tauri 构建：`pnpm install && pnpm run prebuild && pnpm tauri build`

## 许可证

继承上游的 [GPL-3.0](./LICENSE)。上游项目版权归原作者及所有贡献者所有。

# Flow Clash 1.0.0 — Windows x64 编译指南

应用身份 `dev.flowclash.rev`,版本 1.0.0。本包已剔除 macOS 构建产物(target/node_modules),
包含全部源码、图标(flowclash.icns 正常版 + flowclash-dark.icns 暗黑版)、geo 资源与锁定文件。

## 1. 环境准备(一次性,约 7–8 GB 下载)

管理员 PowerShell:

```powershell
winget install OpenJS.NodeJS.LTS Git.Git Rustlang.Rustup
winget install Microsoft.VisualStudio.2022.BuildTools --override "--add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"
rustup default 1.99.0
corepack enable
```

## 2. 网络代理(prebuild 需从 GitHub 下载 Windows 版 mihomo 内核)

```powershell
$env:HTTP_PROXY  = "http://127.0.0.1:<代理端口>"
$env:HTTPS_PROXY = "http://127.0.0.1:<代理端口>"
```

## 3. 依赖安装

```powershell
cd <解压目录>
pnpm install --frozen-lockfile
```

## 4. 预构建(下载 Windows sidecar + 校验资源)

```powershell
pnpm run prebuild x86_64-pc-windows-msvc
```

## 5. 编译打包

```powershell
pnpm tauri build --target x86_64-pc-windows-msvc
```

产物在 `src-tauri\target\x86_64-pc-windows-msvc\release\bundle\nsis\`:
- `Flow Clash_1.0.0_x64_zh-CN.exe`(NSIS 安装包,含简中/英/俄三语)
- WebView2 缺失时由安装器内嵌引导自动安装

## 常见问题

- **链接器报错/找不到 MSVC**:确认 VS Build Tools 装了 "使用 C++ 的桌面开发" 工作负载,并新开终端。
- **cargo 拉依赖超时**:配置 crates.io 镜像或让 cargo 走同一代理(`$env:CARGO_HTTP_PROXY`)。
- **prebuild 下载失败**:检查代理;下载物是 verge-mihomo 的 Windows 版与 geo 数据,放行 github.com 即可。
- **仅快速编译看能不能过**:把第 5 步换成 `pnpm tauri build --target x86_64-pc-windows-msvc --no-bundle`,先出 exe 再出安装包。
- 编译完成后如遇问题,把 `src-tauri\target\...` 下完整的报错文本发给开发者即可。

## 验证安装包

安装后:应用名 Flow Clash,数据目录 `%APPDATA%\dev.flowclash.rev`,服务名 flowclash_service,
默认混合端口 17970、控制端口 19097,不与 Clash Verge Rev 冲突。

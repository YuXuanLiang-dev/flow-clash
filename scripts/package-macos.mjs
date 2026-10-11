import { execFileSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const output = path.resolve(root, '..', 'dist')
const arch = process.arch === 'arm64' ? 'arm64' : 'x64'
const env = { ...process.env, APPLE_SIGNING_IDENTITY: '-' }
const localRust = path.resolve(root, '..', '.tools/rust')
if (fs.existsSync(path.join(localRust, 'cargo/bin/cargo'))) {
  env.CARGO_HOME = path.join(localRust, 'cargo')
  env.RUSTUP_HOME = path.join(localRust, 'rustup')
  env.PATH = `${path.join(localRust, 'cargo/bin')}:${env.PATH}`
}
for (const key of [
  'APPLE_ID',
  'APPLE_PASSWORD',
  'APPLE_TEAM_ID',
  'APPLE_API_KEY',
  'APPLE_API_ISSUER',
  'APPLE_API_KEY_PATH',
])
  delete env[key]

if (process.platform !== 'darwin')
  throw new Error('macOS packaging requires macOS')

const run = (command, args) =>
  execFileSync(command, args, { cwd: root, env, stdio: 'inherit' })
const services = [
  'flowclash-service',
  'flowclash-service-install',
  'flowclash-service-uninstall',
]
if (!process.argv.includes('--bundle-only')) {
  run('cargo', [
    'build',
    '--manifest-path',
    'vendor/clash-verge-service-ipc/Cargo.toml',
    '--target-dir',
    'target/bundled-service',
    '--release',
    '--features',
    'standalone,client',
    ...services.flatMap((name) => ['--bin', name]),
    '--offline',
    '--locked',
  ])
}
for (const name of services) {
  const destination = path.join(root, 'src-tauri/resources', name)
  fs.copyFileSync(
    path.join(root, 'target/bundled-service/release', name),
    destination,
  )
  fs.chmodSync(destination, 0o755)
}

const bundleOnly = process.argv.includes('--bundle-only')
run(process.execPath, [
  'node_modules/@tauri-apps/cli/tauri.js',
  bundleOnly ? 'bundle' : 'build',
  '--ci',
  '--bundles',
  'app',
  ...(bundleOnly ? [] : ['--', '--offline', '--locked']),
])

const app = path.join(root, 'target/release/bundle/macos/Flow Clash.app')
const plist = path.join(app, 'Contents/Info.plist')
const readPlist = (key, file = plist) =>
  execFileSync('/usr/libexec/PlistBuddy', ['-c', `Print :${key}`, file], {
    encoding: 'utf8',
  }).trim()
if (readPlist('CFBundleIdentifier') !== 'dev.flowclash.rev')
  throw new Error('Unexpected application identity')
run('/usr/bin/codesign', ['--verify', '--deep', '--strict', '--verbose=2', app])
const dmgDirectory = path.join(root, 'target/release/bundle/dmg')
fs.mkdirSync(dmgDirectory, { recursive: true })
const version = readPlist('CFBundleShortVersionString')
const dmg = path.join(dmgDirectory, `Flow Clash_${version}_${arch}.dmg`)
const staging = fs.mkdtempSync(path.join(root, 'target/release/installer-'))
try {
  run('/usr/bin/ditto', [app, path.join(staging, 'Flow Clash.app')])
  fs.symlinkSync('/Applications', path.join(staging, 'Applications'))
  run('/usr/bin/hdiutil', [
    'create',
    '-srcfolder',
    staging,
    '-volname',
    'Flow Clash',
    '-fs',
    'HFS+',
    '-format',
    'UDZO',
    '-ov',
    dmg,
  ])
} finally {
  fs.rmSync(staging, { recursive: true, force: true })
}
run('/usr/bin/hdiutil', ['verify', dmg])
const mount = fs.mkdtempSync(path.join(root, 'target/release/installer-check-'))
let mounted = false
try {
  run('/usr/bin/hdiutil', [
    'attach',
    '-readonly',
    '-nobrowse',
    '-noautoopen',
    '-mountpoint',
    mount,
    dmg,
  ])
  mounted = true
  const mountedApp = path.join(mount, 'Flow Clash.app')
  const mountedPlist = path.join(mountedApp, 'Contents/Info.plist')
  if (readPlist('CFBundleIdentifier', mountedPlist) !== 'dev.flowclash.rev')
    throw new Error('Installer is missing the Flow Clash application')
  const executable = path.join(
    'Contents/MacOS',
    readPlist('CFBundleExecutable'),
  )
  const hash = (file) =>
    createHash('sha256').update(fs.readFileSync(file)).digest('hex')
  if (
    hash(path.join(app, executable)) !== hash(path.join(mountedApp, executable))
  )
    throw new Error('Installer application does not match the release build')
  if (fs.readlinkSync(path.join(mount, 'Applications')) !== '/Applications')
    throw new Error('Installer is missing the Applications shortcut')
  run('/usr/bin/codesign', ['--verify', '--deep', '--strict', mountedApp])
} finally {
  if (mounted) run('/usr/bin/hdiutil', ['detach', mount])
  fs.rmSync(mount, { recursive: true, force: true })
}

fs.mkdirSync(output, { recursive: true })
const previous = path.join(output, 'Flow Clash.app')
if (fs.existsSync(previous)) {
  const previousPlist = path.join(previous, 'Contents/Info.plist')
  const previousVersion = readPlist('CFBundleShortVersionString', previousPlist)
  const previousIdentity = readPlist('CFBundleIdentifier', previousPlist)
  if (previousIdentity !== 'dev.flowclash.rev' || previousVersion !== version) {
    const archiveName =
      previousIdentity === 'dev.flowclash.rev'
        ? `${previousVersion}-${new Date().toISOString().replaceAll(':', '-')}`
        : previousVersion
    const archive = path.join(output, 'legacy', archiveName)
    if (fs.existsSync(archive))
      throw new Error(`Archive already exists: ${archive}`)
    fs.mkdirSync(archive, { recursive: true })
    for (const name of [
      'Flow Clash.app',
      `Flow Clash-macOS-${arch}.zip`,
      `Flow Clash-macOS-${arch}.dmg`,
      'SHA256SUMS.txt',
    ]) {
      const existing = path.join(output, name)
      if (fs.existsSync(existing))
        fs.renameSync(existing, path.join(archive, name))
    }
  } else {
    fs.rmSync(previous, { recursive: true })
  }
}

run('/usr/bin/ditto', [app, previous])
const dmgName = `Flow Clash-macOS-${arch}.dmg`
const zipName = `Flow Clash-macOS-${arch}.zip`
fs.copyFileSync(dmg, path.join(output, dmgName))
run('/usr/bin/ditto', [
  '-c',
  '-k',
  '--sequesterRsrc',
  '--keepParent',
  previous,
  path.join(output, zipName),
])
const checksums = [dmgName, zipName].map(
  (name) =>
    `${createHash('sha256')
      .update(fs.readFileSync(path.join(output, name)))
      .digest('hex')}  ${name}`,
)
fs.writeFileSync(
  path.join(output, 'SHA256SUMS.txt'),
  `${checksums.join('\n')}\n`,
)
console.log(
  `Packaged Flow Clash ${readPlist('CFBundleShortVersionString')} (${arch}) in ${output}`,
)

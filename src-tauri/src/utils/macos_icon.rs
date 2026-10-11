use tauri::{AppHandle, Manager as _};

/// Swap the Dock/app icon between the bundled default and the dark variant
/// as the system appearance changes. `None` restores `CFBundleIconFile`.
#[cfg(target_os = "macos")]
pub fn apply_for_theme(app: &AppHandle, dark: bool) {
    use objc2::AnyThread as _;
    use objc2_app_kit::{NSApplication, NSImage};
    use objc2_foundation::MainThreadMarker;

    let Some(mtm) = MainThreadMarker::new() else {
        return;
    };
    unsafe {
        let ns_app = NSApplication::sharedApplication(mtm);
        if dark {
            let Ok(path) = app.path().resource_dir() else {
                return;
            };
            let icon = path
                .join("icons")
                .join("flowclash-dark.icns")
                ;
            if !icon.exists() {
                return;
            }
            let file = objc2_foundation::NSString::from_str(&icon.to_string_lossy());
            let image = NSImage::initWithContentsOfFile(NSImage::alloc(), &file);
            ns_app.setApplicationIconImage(image.as_deref());
        } else {
            ns_app.setApplicationIconImage(None);
        }
    }
}

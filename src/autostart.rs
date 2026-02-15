use std::path::PathBuf;
use tracing::info;

/// Name of the shortcut placed in the Windows Startup folder.
const SHORTCUT_NAME: &str = "Crypto Lake.lnk";

/// Get the path to the Windows Startup folder shortcut.
fn shortcut_path() -> Option<PathBuf> {
    let appdata = std::env::var("APPDATA").ok()?;
    Some(
        PathBuf::from(appdata)
            .join("Microsoft")
            .join("Windows")
            .join("Start Menu")
            .join("Programs")
            .join("Startup")
            .join(SHORTCUT_NAME),
    )
}

/// Check if auto-start is currently enabled.
pub fn is_autostart_enabled() -> bool {
    shortcut_path()
        .map(|p| p.exists())
        .unwrap_or(false)
}

/// Install auto-start by creating a shortcut in the Windows Startup folder.
///
/// Uses PowerShell to create a `.lnk` file pointing to the current executable.
pub fn install_autostart() -> Result<(), String> {
    let exe_path = std::env::current_exe()
        .map_err(|e| format!("Failed to get current exe path: {}", e))?;
    let exe_dir = exe_path
        .parent()
        .ok_or("Failed to get exe directory")?
        .to_string_lossy()
        .to_string();
    let exe_str = exe_path.to_string_lossy().to_string();

    let shortcut = shortcut_path()
        .ok_or("Failed to get Startup folder path")?;
    let shortcut_str = shortcut.to_string_lossy().to_string();

    // PowerShell script to create a .lnk shortcut
    let ps_script = format!(
        r#"$ws = New-Object -ComObject WScript.Shell; $s = $ws.CreateShortcut('{}'); $s.TargetPath = '{}'; $s.WorkingDirectory = '{}'; $s.Description = 'Crypto Lake Data Collector'; $s.Save()"#,
        shortcut_str.replace('\'', "''"),
        exe_str.replace('\'', "''"),
        exe_dir.replace('\'', "''"),
    );

    let output = std::process::Command::new("powershell")
        .args(["-NoProfile", "-Command", &ps_script])
        .output()
        .map_err(|e| format!("Failed to run PowerShell: {}", e))?;

    if output.status.success() {
        info!("Auto-start installed: {}", shortcut_str);
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("PowerShell error: {}", stderr))
    }
}

/// Remove auto-start by deleting the shortcut from the Windows Startup folder.
pub fn remove_autostart() -> Result<(), String> {
    let shortcut = shortcut_path()
        .ok_or("Failed to get Startup folder path")?;

    if shortcut.exists() {
        std::fs::remove_file(&shortcut)
            .map_err(|e| format!("Failed to delete shortcut: {}", e))?;
        info!("Auto-start removed: {}", shortcut.to_string_lossy());
    } else {
        info!("Auto-start was not installed");
    }

    Ok(())
}

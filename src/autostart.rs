use std::path::PathBuf;
use tracing::info;

const TASK_NAME: &str = "Crypto Lake";

/// Legacy startup-folder shortcut path — kept only for migration cleanup.
fn legacy_shortcut_path() -> Option<PathBuf> {
    let appdata = std::env::var("APPDATA").ok()?;
    Some(
        PathBuf::from(appdata)
            .join("Microsoft")
            .join("Windows")
            .join("Start Menu")
            .join("Programs")
            .join("Startup")
            .join("Crypto Lake.lnk"),
    )
}

/// Check if the Task Scheduler task exists.
pub fn is_autostart_enabled() -> bool {
    std::process::Command::new("schtasks")
        .args(["/Query", "/TN", TASK_NAME])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Install auto-start via Windows Task Scheduler.
///
/// Creates a task that:
/// - Triggers at logon for the current user
/// - Waits 60 seconds before launching (network ready by then)
/// - Only starts if a network connection is available
/// - Restarts up to 3 times on failure (1 min interval)
/// - Runs indefinitely (no execution time limit)
///
/// Migrates away from the legacy Startup folder shortcut if present.
pub fn install_autostart() -> Result<(), String> {
    // Migrate: remove old startup folder shortcut
    if let Some(old) = legacy_shortcut_path() {
        if old.exists() {
            let _ = std::fs::remove_file(&old);
            info!("Removed legacy Startup folder shortcut");
        }
    }

    let exe_path = std::env::current_exe()
        .map_err(|e| format!("Failed to get exe path: {}", e))?;
    let exe_dir = exe_path
        .parent()
        .ok_or("Failed to get exe directory")?;
    let exe_str = exe_path.to_string_lossy();
    let dir_str = exe_dir.to_string_lossy();

    let ps_script = format!(
        r#"
$action  = New-ScheduledTaskAction -Execute '{exe}' -WorkingDirectory '{dir}'
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$trigger.Delay = 'PT60S'
$settings = New-ScheduledTaskSettingsSet `
    -RunOnlyIfNetworkAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew
Register-ScheduledTask `
    -TaskName '{name}' `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Force | Out-Null
"#,
        exe = exe_str.replace('\'', "''"),
        dir = dir_str.replace('\'', "''"),
        name = TASK_NAME,
    );

    let output = std::process::Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", &ps_script])
        .output()
        .map_err(|e| format!("Failed to run PowerShell: {}", e))?;

    if output.status.success() {
        info!("Auto-start installed via Task Scheduler (60s delay, network-gated)");
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        Err(format!("PowerShell error: {}{}", stderr, stdout))
    }
}

/// Remove the Task Scheduler task (and legacy shortcut if still present).
pub fn remove_autostart() -> Result<(), String> {
    let output = std::process::Command::new("schtasks")
        .args(["/Delete", "/TN", TASK_NAME, "/F"])
        .output()
        .map_err(|e| format!("Failed to run schtasks: {}", e))?;

    if output.status.success() {
        info!("Auto-start task removed");
    } else {
        info!("Auto-start task was not installed");
    }

    if let Some(old) = legacy_shortcut_path() {
        if old.exists() {
            let _ = std::fs::remove_file(&old);
            info!("Removed legacy Startup folder shortcut");
        }
    }

    Ok(())
}

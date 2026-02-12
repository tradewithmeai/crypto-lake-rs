use crate::health::HealthCounters;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
use tray_icon::{Icon, TrayIconBuilder};

/// Run the system tray icon on the current thread.
/// Blocks until the user clicks Quit.
pub fn run(
    counters: Arc<HealthCounters>,
    exchanges: Vec<String>,
    shutdown: Arc<AtomicBool>,
    server_port: u16,
) {
    // Build context menu
    let title_item = MenuItem::new("Crypto Lake Collector", false, None);
    let dashboard_item = MenuItem::new("Open Dashboard", true, None);
    let status_item = MenuItem::new("Starting...", false, None);
    let exchanges_text = format!("Exchanges: {}", exchanges.join(", "));
    let exchanges_item = MenuItem::new(&exchanges_text, false, None);
    let quit_item = MenuItem::new("Quit", true, None);

    let menu = Menu::new();
    let _ = menu.append(&title_item);
    let _ = menu.append(&dashboard_item);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&status_item);
    let _ = menu.append(&exchanges_item);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&quit_item);

    let icon = create_icon();

    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_icon(icon)
        .with_tooltip("Crypto Lake - Starting...")
        .build()
        .expect("Failed to create system tray icon");

    let quit_id = quit_item.id().clone();
    let dashboard_id = dashboard_item.id().clone();
    let menu_rx = MenuEvent::receiver();
    let start = Instant::now();
    let mut last_update = Instant::now() - Duration::from_secs(10);

    loop {
        // Pump Windows messages so the tray menu works
        pump_win_messages();

        match menu_rx.try_recv() {
            Ok(event) => {
                if event.id == quit_id {
                    shutdown.store(true, Ordering::SeqCst);
                    break;
                } else if event.id == dashboard_id {
                    let url = format!("http://localhost:{}", server_port);
                    let _ = std::process::Command::new("cmd")
                        .args(["/C", "start", "", &url])
                        .spawn();
                }
            }
            Err(_) => {}
        }

        std::thread::sleep(Duration::from_millis(50));

        // Update status every 5 seconds
        if last_update.elapsed() >= Duration::from_secs(5) {
            last_update = Instant::now();

            let trades = counters.trades_received.load(Ordering::Relaxed);
            let bars = counters.bars_produced.load(Ordering::Relaxed);
            let messages = counters.messages_received.load(Ordering::Relaxed);
            let disconnects = counters.ws_disconnects.load(Ordering::Relaxed);
            let bytes_recv = counters.bytes_received.load(Ordering::Relaxed);
            let bytes_disk = counters.bytes_written.load(Ordering::Relaxed);
            let uptime = start.elapsed().as_secs();

            let status_text = format!(
                "Trades: {} | Bars: {} | Up: {}",
                format_count(trades),
                format_count(bars),
                format_uptime(uptime),
            );
            let _ = status_item.set_text(&status_text);

            // Calculate hourly rates
            let hours = if uptime > 0 { uptime as f64 / 3600.0 } else { 1.0 };
            let net_per_hr = bytes_recv as f64 / hours;
            let disk_per_hr = bytes_disk as f64 / hours;

            let mut tooltip = format!(
                "Crypto Lake\n\
                 {} msgs | {} trades | {} bars\n\
                 Uptime: {}\n\
                 Net: {} ({}/hr)\n\
                 Disk: {} ({}/hr)",
                format_count(messages),
                format_count(trades),
                format_count(bars),
                format_uptime(uptime),
                format_bytes(bytes_recv),
                format_bytes(net_per_hr as u64),
                format_bytes(bytes_disk),
                format_bytes(disk_per_hr as u64),
            );
            if disconnects > 0 {
                tooltip.push_str(&format!("\n{} reconnects", disconnects));
            }
            let _ = tray.set_tooltip(Some(&tooltip));
        }
    }

    // Give collector time to flush buffers
    std::thread::sleep(Duration::from_secs(3));
}

/// Create a 32x32 candlestick chart icon matching the favicon.
fn create_icon() -> Icon {
    let size = 32u32;
    let mut rgba = vec![0u8; (size * size * 4) as usize];

    let bg: [u8; 4] = [19, 23, 34, 255];       // #131722
    let green: [u8; 4] = [38, 166, 154, 255];   // #26a69a
    let red: [u8; 4] = [239, 83, 80, 255];      // #ef5350
    let grid: [u8; 4] = [30, 34, 45, 255];      // #1e222d

    // Fill rounded rect background
    let radius = 6.0f32;
    let pad = 2;
    for y in pad..size - pad {
        for x in pad..size - pad {
            let idx = ((y * size + x) * 4) as usize;
            // Rounded corners
            let in_corner = |cx: f32, cy: f32| -> bool {
                let dx = x as f32 - cx;
                let dy = y as f32 - cy;
                (dx * dx + dy * dy).sqrt() <= radius
            };
            let inside = if x < pad + radius as u32 && y < pad + radius as u32 {
                in_corner(pad as f32 + radius, pad as f32 + radius)
            } else if x >= size - pad - radius as u32 && y < pad + radius as u32 {
                in_corner(size as f32 - pad as f32 - radius - 1.0, pad as f32 + radius)
            } else if x < pad + radius as u32 && y >= size - pad - radius as u32 {
                in_corner(pad as f32 + radius, size as f32 - pad as f32 - radius - 1.0)
            } else if x >= size - pad - radius as u32 && y >= size - pad - radius as u32 {
                in_corner(size as f32 - pad as f32 - radius - 1.0, size as f32 - pad as f32 - radius - 1.0)
            } else {
                true
            };
            if inside {
                rgba[idx..idx + 4].copy_from_slice(&bg);
            }
        }
    }

    // Draw candlesticks: (x_center, wick_top, body_top, body_bot, wick_bot, color)
    let candles: [(u32, u32, u32, u32, u32, &[u8; 4]); 4] = [
        (6,  10, 12, 20, 22, &green), // candle 1: green
        (13, 8,  10, 19, 23, &red),   // candle 2: red
        (20, 6,  8,  16, 18, &green), // candle 3: green tall
        (27, 7,  9,  14, 17, &green), // candle 4: green
    ];

    for (cx, wt, bt, bb, wb, color) in &candles {
        // Wick (1px wide)
        for y in *wt..=*wb {
            let idx = ((y * size + cx) * 4) as usize;
            rgba[idx..idx + 4].copy_from_slice(*color);
        }
        // Body (5px wide, centered on cx)
        let bx0 = cx.saturating_sub(2);
        let bx1 = (cx + 2).min(size - 1);
        for y in *bt..=*bb {
            for x in bx0..=bx1 {
                let idx = ((y * size + x) * 4) as usize;
                rgba[idx..idx + 4].copy_from_slice(*color);
            }
        }
    }

    // Baseline
    let y_base = 25u32;
    for x in 4..28 {
        let idx = ((y_base * size + x) * 4) as usize;
        rgba[idx..idx + 4].copy_from_slice(&grid);
    }

    Icon::from_rgba(rgba, size, size).expect("Failed to create icon")
}

fn format_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Pump the Windows message queue so the tray context menu works.
fn pump_win_messages() {
    #[repr(C)]
    struct MSG {
        hwnd: *mut std::ffi::c_void,
        message: u32,
        w_param: usize,
        l_param: isize,
        time: u32,
        pt_x: i32,
        pt_y: i32,
    }

    extern "system" {
        fn PeekMessageW(msg: *mut MSG, hwnd: *mut std::ffi::c_void, min: u32, max: u32, remove: u32) -> i32;
        fn TranslateMessage(msg: *const MSG) -> i32;
        fn DispatchMessageW(msg: *const MSG) -> isize;
    }

    unsafe {
        let mut msg = std::mem::zeroed::<MSG>();
        // PM_REMOVE = 0x0001
        while PeekMessageW(&mut msg, std::ptr::null_mut(), 0, 0, 0x0001) != 0 {
            TranslateMessage(&msg);
            DispatchMessageW(&msg);
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1_024 {
        format!("{:.0} KB", bytes as f64 / 1_024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn format_uptime(secs: u64) -> String {
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    if hours > 24 {
        let days = hours / 24;
        let rem_hours = hours % 24;
        format!("{}d {}h", days, rem_hours)
    } else if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else {
        format!("{}m", mins)
    }
}

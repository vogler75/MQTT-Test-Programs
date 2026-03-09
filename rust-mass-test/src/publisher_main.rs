mod config;
mod metrics;
mod producer;
mod topic;
mod ui;

use crate::config::Config;
use crate::metrics::GlobalMetrics;
use crate::ui::{draw_config_screen, LogBuffer, UIContext};
use clap::Parser;
use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Parser, Debug)]
#[command(name = "MQTT Test")]
#[command(about = "Fast MQTT test program with console GUI", long_about = None)]
struct Args {
    /// MQTT broker host
    #[arg(long)]
    broker: Option<String>,

    /// MQTT broker port
    #[arg(long)]
    port: Option<u16>,

    /// Configuration file to load (JSON)
    #[arg(long)]
    config: Option<String>,

    /// Auto-start without UI (use config file)
    #[arg(long)]
    auto_start: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load or create config
    // If no config file specified, try to load config.json if it exists
    let config_file = if args.config.is_some() {
        args.config.as_deref()
    } else {
        // Try to use config.json if it exists in current directory
        if std::path::Path::new("config.json").exists() {
            Some("config.json")
        } else {
            None
        }
    };

    let mut config = Config::load_or_default(config_file);
    if let Some(broker) = args.broker { config.broker_host = broker; }
    if let Some(port) = args.port { config.broker_port = port; }

    // Notify user if config was loaded
    if config_file.is_some() {
        eprintln!("✅ Loaded configuration from: {}", config_file.unwrap());
    }

    if args.auto_start {
        run_producers(&config).await?;
    } else {
        run_ui(&config).await?;
    }

    Ok(())
}

async fn run_ui(initial_config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Clear screen and hide cursor
    terminal.clear()?;
    terminal.hide_cursor()?;

    let mut ui_ctx = UIContext::new();
    ui_ctx.config = initial_config.clone();

    loop {
        // Configuration loop - can run multiple tests
        let mut should_exit = false;
        loop {
            // Draw configuration screen
            terminal.draw(|f| draw_config_screen(f, &ui_ctx))?;

            // Handle input
            if let Some(should_start) = ui::handle_ui_input(&mut ui_ctx).await {
                if should_start {
                    // Start producers - switch out of raw mode for metrics screen
                    disable_raw_mode()?;
                    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
                    terminal.show_cursor()?;
                    break;
                } else {
                    // Q was pressed - exit application
                    should_exit = true;
                    break;
                }
            }
        }

        if should_exit {
            // Cleanup terminal before exit
            disable_raw_mode()?;
            crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
            terminal.show_cursor()?;
            eprintln!("\n👋 Goodbye!");
            return Ok(());
        }

        // Run producers
        run_producers_with_ui(&ui_ctx.config).await?;

        // Restore terminal for config screen
        eprintln!("\n📋 Returning to configuration screen...");
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;

        // Reset UI state for next test
        ui_ctx.state = ui::UIState::ConfigInput;
        ui_ctx.field_index = 0;
        ui_ctx.input_buffer.clear();
        ui_ctx.in_edit_mode = false;

        // CRITICAL: Completely reset terminal state
        // Drop old terminal
        drop(terminal);

        // Disable raw mode if still active
        let _ = disable_raw_mode();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Enable raw mode
        enable_raw_mode()?;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Create fresh stdout and enter alternate screen
        let mut stdout = io::stdout();

        // Clear any buffered data
        use std::io::Write;
        let _ = stdout.flush();

        // Enter alternate screen
        crossterm::execute!(stdout, EnterAlternateScreen)?;

        // Create completely new terminal
        let backend = CrosstermBackend::new(stdout);
        terminal = Terminal::new(backend)?;

        // Prepare screen
        terminal.clear()?;
        terminal.hide_cursor()?;

        // Wait for terminal to fully stabilize
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Drain any queued events
        loop {
            if !crossterm::event::poll(std::time::Duration::from_millis(100))? {
                break;
            }
            let _ = crossterm::event::read();
        }

        // Wait a bit more
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

async fn run_producers_with_ui(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(config.clone());
    let metrics = Arc::new(Mutex::new(GlobalMetrics::new(config.num_producers)));
    let log_buffer = LogBuffer::new(1000); // Keep last 1000 log lines

    println!("\n📊 Starting {} producers...", config.num_producers);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (pause_tx, pause_rx) = tokio::sync::watch::channel(false);
    let mut is_paused = false;

    let mut handles: Vec<JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>> =
        Vec::new();

    for producer_id in 0..config.num_producers {
        let config_clone = config.clone();
        let client_metrics = metrics.lock().unwrap().clients[producer_id].clone();
        let shutdown_rx_clone = shutdown_rx.clone();
        let pause_rx_clone = pause_rx.clone();
        let log_buffer_clone = log_buffer.clone();

        let handle: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> =
            tokio::spawn(async move {
            crate::producer::run_producer(producer_id, config_clone, Arc::new(client_metrics), shutdown_rx_clone, pause_rx_clone, log_buffer_clone)
                .await
        });

        handles.push(handle);
    }

    // Small delay to let producers connect
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    println!("✅ All producers connected!");
    println!("📊 Producers running (press P to pause/resume, C to clear, Q to quit)...");

    // Enable raw mode to capture keyboard input
    enable_raw_mode()?;

    // Run without TUI - just let producers run and show logs from background
    let mut metrics_timer = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut last_log_index = 0;
    loop {
        tokio::select! {
            _ = metrics_timer.tick() => {
                // Print any new logs first
                let logs = log_buffer.get_logs();
                if last_log_index < logs.len() {
                    let _ = disable_raw_mode();
                    for log in &logs[last_log_index..] {
                        println!("{}", log);
                    }
                    let _ = enable_raw_mode();
                    last_log_index = logs.len();
                }

                // Print metrics every second
                let metrics_guard = metrics.lock().unwrap();
                let total_published = metrics_guard.get_total_published();
                let total_vps = metrics_guard.get_total_vps();
                let connected_clients = metrics_guard.get_connected_count();
                let total_clients = metrics_guard.clients.len();
                let status = if is_paused { "⏸️  PAUSED" } else { "▶️  Running" };

                // Temporarily disable raw mode to print metrics properly
                let _ = disable_raw_mode();
                println!("📈 Connected: {}/{} clients | Published: {} | v/s: {:.2} | {}", connected_clients, total_clients, total_published, total_vps, status);
                let _ = enable_raw_mode();

                // Debug: Show individual client states
                if connected_clients != total_clients && total_clients <= 10 {
                    let _ = disable_raw_mode();
                    for (idx, client) in metrics_guard.clients.iter().enumerate() {
                        let conn_status = if client.is_connected() { "✅" } else { "❌" };
                        println!("  [DEBUG] Client {}: {} | Pub: {}", idx + 1, conn_status, client.get_total_published());
                    }
                    let _ = enable_raw_mode();
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                // Check for keyboard input
                if event::poll(Duration::from_millis(0)).ok().unwrap_or(false) {
                    if let Ok(Event::Key(key)) = event::read() {
                        match key.code {
                            KeyCode::Char('p') | KeyCode::Char('P') => {
                                is_paused = !is_paused;
                                let _ = pause_tx.send(is_paused);
                                let status = if is_paused { "⏸️  Publishing PAUSED" } else { "▶️  Publishing RESUMED" };
                                let _ = disable_raw_mode();
                                println!("{}", status);
                                let _ = enable_raw_mode();
                            }
                            KeyCode::Char('c') | KeyCode::Char('C') => {
                                let _ = disable_raw_mode();
                                println!("🧹 Metrics cleared");
                                let _ = enable_raw_mode();
                                // Reset all metrics
                                metrics.lock().unwrap().reset();
                            }
                            KeyCode::Char('q') | KeyCode::Char('Q') => {
                                let _ = disable_raw_mode();
                                println!("⏹️  Stopping producers...");
                                let _ = enable_raw_mode();
                                let _ = shutdown_tx.send(true);
                                break;
                            }
                            _ => {}
                        }
                    }
                }

                // Check if any producer crashed
                let mut any_crashed = false;
                for handle in &handles {
                    if handle.is_finished() {
                        any_crashed = true;
                        break;
                    }
                }
                if any_crashed {
                    let _ = disable_raw_mode();
                    println!("⚠️  A producer crashed, stopping test...");
                    let _ = enable_raw_mode();
                    let _ = shutdown_tx.send(true); // Send shutdown signal
                    break;
                }
            }
        }
    }

    // Disable raw mode before final output
    disable_raw_mode()?;

    println!("📊 Stopping all producers...");

    // Abort all tasks (as a fallback, if graceful shutdown fails)
    for handle in handles {
        handle.abort();
    }

    // Wait a bit for tasks to finish
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("✅ Test completed!");
    println!("Total messages published: {}", metrics.lock().unwrap().get_total_published());

    Ok(())
}

async fn run_producers(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(config.clone());
    let metrics = Arc::new(Mutex::new(GlobalMetrics::new(config.num_producers)));
    let log_buffer = LogBuffer::new(100); // Keep last 100 log lines

    eprintln!("Starting {} producers...", config.num_producers);

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (_pause_tx, pause_rx) = tokio::sync::watch::channel(false);

    let mut handles: Vec<JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>> =
        Vec::new();

    for producer_id in 0..config.num_producers {
        let config_clone = config.clone();
        let client_metrics = metrics.lock().unwrap().clients[producer_id].clone();
        let shutdown_rx_clone = shutdown_rx.clone();
        let pause_rx_clone = pause_rx.clone();
        let log_buffer_clone = log_buffer.clone();

        let handle: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> =
            tokio::spawn(async move {
            crate::producer::run_producer(producer_id, config_clone, Arc::new(client_metrics), shutdown_rx_clone, pause_rx_clone, log_buffer_clone)
                .await
        });

        handles.push(handle);
    }

    // Wait for all tasks to finish (they should exit gracefully on shutdown signal)
    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}

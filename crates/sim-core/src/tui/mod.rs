mod config_panel;
mod dashboard;
mod devices;
mod wizard;

use std::io;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::{execute, terminal::ClearType};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph, Tabs};
use ratatui::Terminal;
use tokio::runtime::Runtime;

use crate::app::snapshot::{build_snapshot, device_points, AppSnapshot};
use crate::app::{restart_process, AppContext};
use crate::config::SimulatorConfig;

use self::config_panel::ConfigPanelState;
use self::devices::DevicesState;
use self::wizard::WizardState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    Status,
    Devices,
    Config,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Modal {
    None,
    ConfirmReset,
    PostSave,
    Wizard,
}

pub struct TuiState {
    tab: Tab,
    modal: Modal,
    devices: DevicesState,
    config_panel: ConfigPanelState,
    wizard: WizardState,
    should_quit: bool,
    snapshot: AppSnapshot,
    detail_points: Vec<crate::app::snapshot::PointRow>,
}

impl TuiState {
    fn new(snapshot: AppSnapshot) -> Self {
        Self {
            tab: Tab::Status,
            modal: Modal::None,
            devices: DevicesState::default(),
            config_panel: ConfigPanelState::default(),
            wizard: WizardState::default(),
            should_quit: false,
            snapshot,
            detail_points: Vec::new(),
        }
    }
}

pub fn run(rt: Runtime, ctx: AppContext) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(
        stdout,
        EnterAlternateScreen,
        crossterm::terminal::Clear(ClearType::All)
    )?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let handle = rt.handle().clone();
    let result = run_loop(&mut terminal, &handle, &ctx);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    handle: &tokio::runtime::Handle,
    ctx: &AppContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = handle.block_on(build_snapshot(
        &ctx.meta,
        &ctx.simulation,
        &ctx.metrics,
        ctx.log.lines(),
    ));
    let mut state = TuiState::new(snapshot);

    loop {
        handle.block_on(async {
            state.snapshot =
                build_snapshot(&ctx.meta, &ctx.simulation, &ctx.metrics, ctx.log.lines()).await;
        });

        if state.devices.detail_device_id.is_some() && state.tab == Tab::Devices {
            if let Some(id) = state.devices.detail_device_id {
                state.detail_points = handle.block_on(async {
                    let sim = ctx.simulation.lock().await;
                    device_points(&sim, id)
                });
            }
        }

        terminal.draw(|f| draw_ui(f, &state))?;

        if state.should_quit {
            break;
        }

        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
                    state.should_quit = true;
                    continue;
                }
                handle_key(&mut state, key.code, ctx)?;
            }
        }
    }

    Ok(())
}

fn draw_ui(f: &mut ratatui::Frame, state: &TuiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(3)])
        .split(f.area());

    let titles = vec![" Status ", " Devices ", " Config "];
    let selected = match state.tab {
        Tab::Status => 0,
        Tab::Devices => 1,
        Tab::Config => 2,
    };
    let tabs = Tabs::new(titles)
        .block(Block::default().title(" Simulator ").borders(Borders::ALL))
        .select(selected)
        .style(Style::default())
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(Color::Cyan),
        );
    f.render_widget(tabs, chunks[0]);

    let inner = chunks[0].inner(ratatui::layout::Margin {
        vertical: 2,
        horizontal: 1,
    });
    match state.tab {
        Tab::Status => dashboard::draw(f, inner, &state.snapshot),
        Tab::Devices => devices::draw(
            f,
            inner,
            &state.devices,
            &state.snapshot,
            &state.detail_points,
        ),
        Tab::Config => config_panel::draw(f, inner, &state.config_panel, &state.snapshot),
    }

    let help = Paragraph::new(Line::from(
        "Tab/←→ switch | ↑↓ scroll | Enter select | / filter | Esc back | q quit",
    ))
    .style(Style::default().fg(Color::DarkGray));
    f.render_widget(help, chunks[1]);

    if state.modal != Modal::None {
        draw_modal(f, state);
    }
}

fn draw_modal(f: &mut ratatui::Frame, state: &TuiState) {
    let area = centered_rect(60, 40, f.area());
    f.render_widget(ratatui::widgets::Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .style(Style::default().bg(Color::Black));

    match state.modal {
        Modal::ConfirmReset => {
            let text = vec![
                Line::from("Overwrite config.yaml with bundled sample?"),
                Line::from(""),
                Line::from("y = confirm   n = cancel"),
            ];
            f.render_widget(Paragraph::new(text).block(block.title(" Confirm ")), area);
        }
        Modal::PostSave => {
            let text = vec![
                Line::from("Config saved successfully."),
                Line::from(""),
                Line::from("R = restart now   Q = quit"),
            ];
            f.render_widget(Paragraph::new(text).block(block.title(" Saved ")), area);
        }
        Modal::Wizard => wizard::draw(f, area, &state.wizard),
        Modal::None => {}
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn handle_key(
    state: &mut TuiState,
    code: KeyCode,
    ctx: &AppContext,
) -> Result<(), Box<dyn std::error::Error>> {
    if state.modal == Modal::PostSave {
        return handle_post_save(state, code);
    }
    if state.modal == Modal::ConfirmReset {
        return handle_confirm_reset(state, code, ctx);
    }
    if state.modal == Modal::Wizard {
        return wizard::handle_key(state, code, ctx);
    }

    match code {
        KeyCode::Char('q') => state.should_quit = true,
        KeyCode::Tab => state.tab = next_tab(state.tab),
        KeyCode::BackTab => state.tab = prev_tab(state.tab),
        KeyCode::Right => state.tab = next_tab(state.tab),
        KeyCode::Left => state.tab = prev_tab(state.tab),
        KeyCode::Esc => {
            state.devices.filter_mode = false;
            state.devices.detail_device_id = None;
        }
        _ => match state.tab {
            Tab::Devices => devices::handle_key(state, code),
            Tab::Config => {
                handle_config_tab(state, code)?;
            }
            Tab::Status => {}
        },
    }
    Ok(())
}

fn handle_config_tab(
    state: &mut TuiState,
    code: KeyCode,
) -> Result<(), Box<dyn std::error::Error>> {
    match code {
        KeyCode::Up => {
            state.config_panel.selected = state.config_panel.selected.saturating_sub(1);
        }
        KeyCode::Down => {
            state.config_panel.selected = (state.config_panel.selected + 1).min(1);
        }
        KeyCode::Enter => match state.config_panel.selected {
            0 => state.modal = Modal::ConfirmReset,
            1 => {
                state.wizard = WizardState::default();
                state.modal = Modal::Wizard;
            }
            _ => {}
        },
        _ => {}
    }
    Ok(())
}

fn handle_confirm_reset(
    state: &mut TuiState,
    code: KeyCode,
    ctx: &AppContext,
) -> Result<(), Box<dyn std::error::Error>> {
    match code {
        KeyCode::Char('y') | KeyCode::Char('Y') => {
            SimulatorConfig::write_default_config(&ctx.meta.config_path)?;
            ctx.log.push("Reset config to bundled sample".to_string());
            state.modal = Modal::PostSave;
        }
        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
            state.modal = Modal::None;
        }
        _ => {}
    }
    Ok(())
}

fn handle_post_save(state: &mut TuiState, code: KeyCode) -> Result<(), Box<dyn std::error::Error>> {
    match code {
        KeyCode::Char('r') | KeyCode::Char('R') => {
            restart_process()?;
            state.should_quit = true;
        }
        KeyCode::Char('q') | KeyCode::Char('Q') => {
            state.should_quit = true;
        }
        _ => {}
    }
    Ok(())
}

fn next_tab(tab: Tab) -> Tab {
    match tab {
        Tab::Status => Tab::Devices,
        Tab::Devices => Tab::Config,
        Tab::Config => Tab::Status,
    }
}

fn prev_tab(tab: Tab) -> Tab {
    match tab {
        Tab::Status => Tab::Config,
        Tab::Devices => Tab::Status,
        Tab::Config => Tab::Devices,
    }
}

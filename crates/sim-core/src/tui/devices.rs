use crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::Frame;

use crate::app::snapshot::{AppSnapshot, PointRow};
use crate::tui::TuiState;

#[derive(Debug, Default)]
pub struct DevicesState {
    pub scroll: usize,
    pub filter: String,
    pub filter_mode: bool,
    pub detail_device_id: Option<u32>,
    pub list_index: usize,
}

pub fn filtered_devices<'a>(
    snap: &'a AppSnapshot,
    filter: &str,
) -> Vec<&'a crate::app::snapshot::DeviceRow> {
    let f = filter.to_lowercase();
    snap.devices
        .iter()
        .filter(|d| {
            f.is_empty()
                || d.name.to_lowercase().contains(&f)
                || d.device_id.to_string().contains(&f)
        })
        .collect()
}

pub fn draw(
    f: &mut Frame,
    area: ratatui::layout::Rect,
    state: &DevicesState,
    snap: &AppSnapshot,
    detail_points: &[PointRow],
) {
    if let Some(device_id) = state.detail_device_id {
        draw_detail(f, area, device_id, detail_points, state);
        return;
    }

    let filtered = filtered_devices(snap, &state.filter);
    let header = Row::new(vec!["Device ID", "Name", "Points"])
        .style(Style::default().add_modifier(Modifier::BOLD))
        .bottom_margin(1);

    let rows: Vec<Row> = filtered
        .iter()
        .enumerate()
        .skip(state.scroll)
        .take(area.height.saturating_sub(4) as usize)
        .map(|(i, d)| {
            let style = if i == state.list_index {
                Style::default().bg(Color::DarkGray)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(d.device_id.to_string()),
                Cell::from(d.name.clone()),
                Cell::from(d.point_count.to_string()),
            ])
            .style(style)
        })
        .collect();

    let title = if state.filter_mode {
        format!(" Devices (filter) ")
    } else if state.filter.is_empty() {
        " Devices ".to_string()
    } else {
        format!(" Devices [filter: {}] ", state.filter)
    };

    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Min(20),
            Constraint::Length(8),
        ],
    )
    .header(header)
    .block(Block::default().title(title).borders(Borders::ALL));

    f.render_widget(table, area);

    if state.filter_mode {
        let input_area = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1), Constraint::Length(3)])
            .split(area)[1];
        let filter_line = Line::from(vec![
            Span::styled("Filter: ", Style::default().fg(Color::Cyan)),
            Span::raw(&state.filter),
            Span::raw("_"),
        ]);
        f.render_widget(
            Paragraph::new(filter_line).block(Block::default().borders(Borders::ALL)),
            input_area,
        );
    }
}

fn draw_detail(
    f: &mut Frame,
    area: ratatui::layout::Rect,
    device_id: u32,
    points: &[PointRow],
    state: &DevicesState,
) {
    let header = Row::new(vec!["Label", "Type", "Value"])
        .style(Style::default().add_modifier(Modifier::BOLD))
        .bottom_margin(1);

    let rows: Vec<Row> = points
        .iter()
        .skip(state.scroll)
        .take(area.height.saturating_sub(4) as usize)
        .map(|p| {
            Row::new(vec![
                Cell::from(p.label.clone()),
                Cell::from(p.object_type.clone()),
                Cell::from(p.value.clone()),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(22),
            Constraint::Length(18),
            Constraint::Min(10),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(format!(" Device {device_id} (Esc back) "))
            .borders(Borders::ALL),
    );

    f.render_widget(table, area);
}

pub fn handle_key(state: &mut TuiState, code: KeyCode) {
    let filtered_len = filtered_devices(&state.snapshot, &state.devices.filter).len();

    if state.devices.filter_mode {
        match code {
            KeyCode::Esc | KeyCode::Enter => state.devices.filter_mode = false,
            KeyCode::Backspace => {
                state.devices.filter.pop();
                state.devices.list_index = 0;
                state.devices.scroll = 0;
            }
            KeyCode::Char(c) => {
                state.devices.filter.push(c);
                state.devices.list_index = 0;
                state.devices.scroll = 0;
            }
            _ => {}
        }
        return;
    }

    if state.devices.detail_device_id.is_some() {
        match code {
            KeyCode::Up => {
                state.devices.scroll = state.devices.scroll.saturating_sub(1);
            }
            KeyCode::Down => {
                state.devices.scroll = state.devices.scroll.saturating_add(1);
            }
            KeyCode::Esc => {
                state.devices.detail_device_id = None;
                state.devices.scroll = 0;
            }
            _ => {}
        }
        return;
    }

    match code {
        KeyCode::Char('/') => state.devices.filter_mode = true,
        KeyCode::Up => {
            if state.devices.list_index > 0 {
                state.devices.list_index -= 1;
            }
            if state.devices.list_index < state.devices.scroll {
                state.devices.scroll = state.devices.list_index;
            }
        }
        KeyCode::Down => {
            if filtered_len > 0 && state.devices.list_index + 1 < filtered_len {
                state.devices.list_index += 1;
            }
            let visible = 20usize;
            if state.devices.list_index >= state.devices.scroll + visible {
                state.devices.scroll = state.devices.list_index.saturating_sub(visible - 1);
            }
        }
        KeyCode::Enter => {
            let filtered = filtered_devices(&state.snapshot, &state.devices.filter);
            if let Some(d) = filtered.get(state.devices.list_index) {
                state.devices.detail_device_id = Some(d.device_id);
                state.devices.scroll = 0;
            }
        }
        _ => {}
    }
}

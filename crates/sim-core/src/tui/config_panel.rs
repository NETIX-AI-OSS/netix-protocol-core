use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::app::snapshot::AppSnapshot;

#[derive(Debug, Default)]
pub struct ConfigPanelState {
    pub selected: usize,
}

pub fn draw(
    f: &mut Frame,
    area: ratatui::layout::Rect,
    state: &ConfigPanelState,
    snap: &AppSnapshot,
) {
    let items = [
        "Reset to bundled sample (Marina Heights Tower)",
        "Create minimal custom config (wizard)",
    ];

    let lines: Vec<Line> = items
        .iter()
        .enumerate()
        .map(|(i, label)| {
            let prefix = if i == state.selected { "> " } else { "  " };
            let style = if i == state.selected {
                Style::default()
                    .add_modifier(Modifier::BOLD)
                    .fg(Color::Cyan)
            } else {
                Style::default()
            };
            Line::from(Span::styled(format!("{prefix}{label}"), style))
        })
        .chain([
            Line::from(""),
            Line::from(vec![
                Span::styled("Config path: ", Style::default().fg(Color::DarkGray)),
                Span::raw(&snap.config_path),
            ]),
            Line::from(""),
            Line::from("Changes are written to disk. Restart required to reload simulation."),
        ])
        .collect();

    f.render_widget(
        Paragraph::new(lines).block(Block::default().title(" Config ").borders(Borders::ALL)),
        area,
    );
}

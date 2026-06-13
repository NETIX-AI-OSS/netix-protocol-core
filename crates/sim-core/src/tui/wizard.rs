use crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::app::AppContext;
use crate::config::minimal::{MinimalConfigOptions, PresetSize};
use crate::config::SimulatorConfig;
use crate::tui::{Modal, TuiState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WizardField {
    BuildingName,
    Location,
    Size,
    Plant,
    Ahus,
    Vavs,
    Meters,
    Save,
}

#[derive(Debug)]
pub struct WizardState {
    pub building_name: String,
    pub location: String,
    pub size: PresetSize,
    pub include_plant: bool,
    pub include_ahus: bool,
    pub include_vavs: bool,
    pub include_meters: bool,
    pub selected: WizardField,
    pub editing_text: bool,
}

impl WizardState {
    pub fn default() -> Self {
        Self {
            building_name: "Custom Building".to_string(),
            location: String::new(),
            size: PresetSize::Small,
            include_plant: true,
            include_ahus: true,
            include_vavs: true,
            include_meters: false,
            selected: WizardField::BuildingName,
            editing_text: false,
        }
    }
}

pub fn draw(f: &mut Frame, area: ratatui::layout::Rect, wizard: &WizardState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(8), Constraint::Length(3)])
        .margin(1)
        .split(area);

    let size_label = match wizard.size {
        PresetSize::Small => "Small",
        PresetSize::Medium => "Medium",
    };

    let fields = [
        (
            WizardField::BuildingName,
            format!("Building name: {}", wizard.building_name),
        ),
        (
            WizardField::Location,
            format!(
                "Location (optional): {}",
                if wizard.location.is_empty() {
                    "—".to_string()
                } else {
                    wizard.location.clone()
                }
            ),
        ),
        (
            WizardField::Size,
            format!("Preset size: {size_label} (Space toggles)"),
        ),
        (
            WizardField::Plant,
            format!(
                "Central plant: {}",
                if wizard.include_plant { "yes" } else { "no" }
            ),
        ),
        (
            WizardField::Ahus,
            format!("AHUs: {}", if wizard.include_ahus { "yes" } else { "no" }),
        ),
        (
            WizardField::Vavs,
            format!("VAVs: {}", if wizard.include_vavs { "yes" } else { "no" }),
        ),
        (
            WizardField::Meters,
            format!(
                "Meters: {}",
                if wizard.include_meters { "yes" } else { "no" }
            ),
        ),
        (WizardField::Save, "Save config".to_string()),
    ];

    let lines: Vec<Line> = fields
        .iter()
        .map(|(field, text)| {
            let marker = if wizard.selected == *field {
                "> "
            } else {
                "  "
            };
            let style = if wizard.selected == *field {
                Style::default()
                    .add_modifier(Modifier::BOLD)
                    .fg(Color::Cyan)
            } else {
                Style::default()
            };
            Line::from(Span::styled(format!("{marker}{text}"), style))
        })
        .collect();

    f.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .title(" Minimal config wizard ")
                .borders(Borders::ALL),
        ),
        chunks[0],
    );

    let help = if wizard.editing_text {
        "Type text | Enter done | Esc cancel"
    } else {
        "↑↓ move | Enter edit/toggle | Space toggle | Esc cancel"
    };
    f.render_widget(
        Paragraph::new(help).style(Style::default().fg(Color::DarkGray)),
        chunks[1],
    );
}

pub fn handle_key(
    state: &mut TuiState,
    code: KeyCode,
    ctx: &AppContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let wizard = &mut state.wizard;

    if wizard.editing_text {
        match code {
            KeyCode::Esc => wizard.editing_text = false,
            KeyCode::Enter => wizard.editing_text = false,
            KeyCode::Backspace => match wizard.selected {
                WizardField::BuildingName => {
                    wizard.building_name.pop();
                }
                WizardField::Location => {
                    wizard.location.pop();
                }
                _ => {}
            },
            KeyCode::Char(c) => match wizard.selected {
                WizardField::BuildingName => wizard.building_name.push(c),
                WizardField::Location => wizard.location.push(c),
                _ => {}
            },
            _ => {}
        }
        return Ok(());
    }

    match code {
        KeyCode::Esc => state.modal = Modal::None,
        KeyCode::Up => wizard.selected = prev_field(wizard.selected),
        KeyCode::Down => wizard.selected = next_field(wizard.selected),
        KeyCode::Char(' ') => toggle_field(wizard),
        KeyCode::Enter => match wizard.selected {
            WizardField::BuildingName | WizardField::Location => wizard.editing_text = true,
            WizardField::Save => {
                let opts = MinimalConfigOptions {
                    building_name: wizard.building_name.clone(),
                    location: if wizard.location.is_empty() {
                        None
                    } else {
                        Some(wizard.location.clone())
                    },
                    size: wizard.size,
                    include_plant: wizard.include_plant,
                    include_ahus: wizard.include_ahus,
                    include_vavs: wizard.include_vavs,
                    include_meters: wizard.include_meters,
                };
                let cfg = SimulatorConfig::from_minimal(&opts)?;
                SimulatorConfig::write_config(&ctx.meta.config_path, &cfg)?;
                ctx.log
                    .push(format!("Wrote minimal config for {}", opts.building_name));
                state.modal = Modal::PostSave;
            }
            _ => toggle_field(wizard),
        },
        _ => {}
    }
    Ok(())
}

fn toggle_field(wizard: &mut WizardState) {
    match wizard.selected {
        WizardField::Size => {
            wizard.size = match wizard.size {
                PresetSize::Small => PresetSize::Medium,
                PresetSize::Medium => PresetSize::Small,
            };
        }
        WizardField::Plant => wizard.include_plant = !wizard.include_plant,
        WizardField::Ahus => wizard.include_ahus = !wizard.include_ahus,
        WizardField::Vavs => wizard.include_vavs = !wizard.include_vavs,
        WizardField::Meters => wizard.include_meters = !wizard.include_meters,
        _ => {}
    }
}

fn next_field(field: WizardField) -> WizardField {
    match field {
        WizardField::BuildingName => WizardField::Location,
        WizardField::Location => WizardField::Size,
        WizardField::Size => WizardField::Plant,
        WizardField::Plant => WizardField::Ahus,
        WizardField::Ahus => WizardField::Vavs,
        WizardField::Vavs => WizardField::Meters,
        WizardField::Meters => WizardField::Save,
        WizardField::Save => WizardField::BuildingName,
    }
}

fn prev_field(field: WizardField) -> WizardField {
    match field {
        WizardField::BuildingName => WizardField::Save,
        WizardField::Location => WizardField::BuildingName,
        WizardField::Size => WizardField::Location,
        WizardField::Plant => WizardField::Size,
        WizardField::Ahus => WizardField::Plant,
        WizardField::Vavs => WizardField::Ahus,
        WizardField::Meters => WizardField::Vavs,
        WizardField::Save => WizardField::Meters,
    }
}

use crate::config::UiTheme;
use iced::widget::{button, column, container, row, svg, text, text_input};
use iced::{Alignment, Background, Border, Color, Element, Length, Padding, Shadow, Theme, Vector};

#[derive(Debug, Clone, Copy)]
pub struct Palette {
    pub background: Color,
    pub sidebar: Color,
    pub surface: Color,
    pub surface_alt: Color,
    pub elevated: Color,
    pub text: Color,
    pub muted: Color,
    pub subtle: Color,
    pub border: Color,
    pub accent: Color,
    pub accent_text: Color,
    pub success: Color,
    pub warning: Color,
    pub danger: Color,
}

#[derive(Debug, Clone, Copy)]
pub enum Icon {
    Brand,
    Overview,
    Discover,
    Points,
    Publish,
    Settings,
    Logs,
    Start,
    Stop,
    Refresh,
    Save,
    Edit,
    Delete,
}

#[derive(Debug, Clone, Copy)]
pub enum ButtonKind {
    Primary,
    Secondary,
    Ghost,
    Danger,
}

#[derive(Debug, Clone, Copy)]
pub enum ChipKind {
    Neutral,
    Accent,
    Success,
    Warning,
    Danger,
}

pub fn palette(theme: UiTheme) -> Palette {
    match theme {
        UiTheme::Light => Palette {
            background: Color::from_rgb8(237, 242, 244),
            sidebar: Color::from_rgb8(14, 28, 35),
            surface: Color::from_rgb8(248, 251, 252),
            surface_alt: Color::from_rgb8(240, 245, 247),
            elevated: Color::WHITE,
            text: Color::from_rgb8(18, 27, 32),
            muted: Color::from_rgb8(85, 101, 111),
            subtle: Color::from_rgb8(125, 143, 153),
            border: Color::from_rgb8(205, 217, 222),
            accent: Color::from_rgb8(3, 137, 148),
            accent_text: Color::WHITE,
            success: Color::from_rgb8(20, 150, 98),
            warning: Color::from_rgb8(190, 126, 23),
            danger: Color::from_rgb8(207, 63, 63),
        },
        UiTheme::Auto | UiTheme::Dark => Palette {
            background: Color::from_rgb8(10, 16, 20),
            sidebar: Color::from_rgb8(8, 22, 28),
            surface: Color::from_rgb8(18, 29, 35),
            surface_alt: Color::from_rgb8(22, 37, 45),
            elevated: Color::from_rgb8(27, 43, 51),
            text: Color::from_rgb8(229, 240, 244),
            muted: Color::from_rgb8(154, 173, 181),
            subtle: Color::from_rgb8(105, 128, 138),
            border: Color::from_rgb8(45, 69, 80),
            accent: Color::from_rgb8(35, 189, 188),
            accent_text: Color::from_rgb8(3, 21, 24),
            success: Color::from_rgb8(73, 205, 139),
            warning: Color::from_rgb8(232, 174, 73),
            danger: Color::from_rgb8(244, 102, 102),
        },
    }
}

pub fn app_style(palette: Palette) -> iced::theme::Style {
    iced::theme::Style {
        background_color: palette.background,
        text_color: palette.text,
    }
}

pub fn brand<'a, Message: 'a>() -> Element<'a, Message> {
    svg(svg::Handle::from_memory(icon_bytes(Icon::Brand)))
        .width(Length::Fixed(206.0))
        .height(Length::Fixed(84.0))
        .into()
}

pub fn icon<'a, Message: 'a>(icon: Icon, color: Color, size: f32) -> Element<'a, Message> {
    svg(svg::Handle::from_memory(icon_bytes(icon)))
        .width(Length::Fixed(size))
        .height(Length::Fixed(size))
        .style(move |_theme: &Theme, _status| svg::Style { color: Some(color) })
        .into()
}

pub fn action_button<'a, Message: Clone + 'a>(
    palette: Palette,
    icon_kind: Icon,
    label: impl Into<String>,
    kind: ButtonKind,
) -> button::Button<'a, Message> {
    let color = match kind {
        ButtonKind::Primary => palette.accent_text,
        ButtonKind::Danger => palette.danger,
        ButtonKind::Secondary | ButtonKind::Ghost => palette.text,
    };
    let content = row![
        icon(icon_kind, color, 15.0),
        text(label.into()).size(14).color(color)
    ]
    .spacing(7)
    .align_y(Alignment::Center);

    button(content)
        .padding(button_padding())
        .style(move |_theme, status| button_style(palette, kind, status))
}

pub fn nav_button<'a, Message: Clone + 'a>(
    palette: Palette,
    icon_kind: Icon,
    label: impl Into<String>,
    active: bool,
) -> button::Button<'a, Message> {
    let color = if active { palette.text } else { palette.muted };
    let content = row![
        icon(icon_kind, color, 17.0),
        text(label.into()).size(14).color(color).width(Length::Fill)
    ]
    .spacing(10)
    .align_y(Alignment::Center);

    button(content)
        .width(Length::Fill)
        .padding(Padding {
            top: 10.0,
            right: 12.0,
            bottom: 10.0,
            left: 12.0,
        })
        .style(move |_theme, status| nav_style(palette, active, status))
}

pub fn card<'a, Message: 'a>(
    palette: Palette,
    content: impl Into<Element<'a, Message>>,
) -> Element<'a, Message> {
    container(content)
        .padding(16)
        .width(Length::Fill)
        .style(move |_| panel_style(palette.elevated, palette.border, 8.0, true))
        .into()
}

pub fn sidebar_style(palette: Palette) -> iced::widget::container::Style {
    panel_style(palette.sidebar, Color::TRANSPARENT, 0.0, false)
}

pub fn status_bar_style(palette: Palette) -> iced::widget::container::Style {
    panel_style(palette.surface, palette.border, 0.0, false)
}

pub fn row_style(palette: Palette) -> iced::widget::container::Style {
    panel_style(palette.surface_alt, palette.border, 6.0, false)
}

pub fn section_title<'a, Message: 'a>(
    palette: Palette,
    label: impl Into<String>,
) -> Element<'a, Message> {
    text(label.into()).size(18).color(palette.text).into()
}

pub fn eyebrow<'a, Message: 'a>(
    palette: Palette,
    label: impl Into<String>,
) -> Element<'a, Message> {
    text(label.into()).size(11).color(palette.subtle).into()
}

pub fn muted<'a, Message: 'a>(palette: Palette, label: impl Into<String>) -> Element<'a, Message> {
    text(label.into()).size(13).color(palette.muted).into()
}

pub fn chip<'a, Message: 'a>(
    palette: Palette,
    label: impl Into<String>,
    kind: ChipKind,
) -> Element<'a, Message> {
    let color = chip_color(palette, kind);
    container(text(label.into()).size(12).color(color))
        .padding(Padding {
            top: 4.0,
            right: 8.0,
            bottom: 4.0,
            left: 8.0,
        })
        .style(move |_| {
            panel_style(
                tint(color, palette.surface, 0.14),
                tint(color, palette.border, 0.45),
                6.0,
                false,
            )
        })
        .into()
}

pub fn metric<'a, Message: 'a>(
    palette: Palette,
    label: impl Into<String>,
    value: impl Into<String>,
    hint: impl Into<String>,
    kind: ChipKind,
) -> Element<'a, Message> {
    let color = chip_color(palette, kind);
    card(
        palette,
        column![
            text(label.into()).size(12).color(palette.subtle),
            text(value.into()).size(26).color(color),
            text(hint.into()).size(12).color(palette.muted)
        ]
        .spacing(6),
    )
}

pub fn labeled_input<'a, Message: Clone + 'a>(
    palette: Palette,
    label: &'a str,
    hint: &'a str,
    value: &'a str,
    on_input: impl Fn(String) -> Message + 'a,
) -> Element<'a, Message> {
    column![
        row![
            text(label).size(13).color(palette.text),
            text(hint).size(12).color(palette.subtle)
        ]
        .spacing(8)
        .align_y(Alignment::Center),
        text_input(label, value)
            .on_input(on_input)
            .padding(Padding {
                top: 9.0,
                right: 10.0,
                bottom: 9.0,
                left: 10.0,
            })
            .size(14)
            .width(Length::Fill)
            .style(move |_theme, status| input_style(palette, status)),
    ]
    .spacing(5)
    .width(Length::Fill)
    .into()
}

pub fn field_readout<'a, Message: 'a>(
    palette: Palette,
    label: impl Into<String>,
    value: impl Into<String>,
) -> Element<'a, Message> {
    column![
        text(label.into()).size(12).color(palette.subtle),
        text(value.into()).size(14).color(palette.text)
    ]
    .spacing(3)
    .width(Length::Fill)
    .into()
}

fn button_style(palette: Palette, kind: ButtonKind, status: button::Status) -> button::Style {
    let (background, border, text_color) = match kind {
        ButtonKind::Primary => (palette.accent, palette.accent, palette.accent_text),
        ButtonKind::Secondary => (palette.surface_alt, palette.border, palette.text),
        ButtonKind::Ghost => (Color::TRANSPARENT, Color::TRANSPARENT, palette.text),
        ButtonKind::Danger => (
            Color::TRANSPARENT,
            tint(palette.danger, palette.border, 0.55),
            palette.danger,
        ),
    };
    let bg = match status {
        button::Status::Hovered => tint(palette.accent, background, 0.16),
        button::Status::Pressed => tint(Color::BLACK, background, 0.08),
        button::Status::Disabled => tint(palette.background, background, 0.55),
        button::Status::Active => background,
    };

    button::Style {
        background: Some(Background::Color(bg)),
        text_color,
        border: Border {
            color: border,
            width: 1.0,
            radius: 6.0.into(),
        },
        shadow: Shadow::default(),
        snap: false,
    }
}

fn nav_style(palette: Palette, active: bool, status: button::Status) -> button::Style {
    let background = if active {
        tint(palette.accent, palette.sidebar, 0.20)
    } else if matches!(status, button::Status::Hovered) {
        tint(palette.surface, palette.sidebar, 0.26)
    } else {
        Color::TRANSPARENT
    };
    let border = if active {
        tint(palette.accent, palette.border, 0.45)
    } else {
        Color::TRANSPARENT
    };

    button::Style {
        background: Some(Background::Color(background)),
        text_color: if active { palette.text } else { palette.muted },
        border: Border {
            color: border,
            width: 1.0,
            radius: 7.0.into(),
        },
        shadow: Shadow::default(),
        snap: false,
    }
}

fn input_style(palette: Palette, status: text_input::Status) -> text_input::Style {
    let focused = matches!(status, text_input::Status::Focused { .. });
    text_input::Style {
        background: Background::Color(palette.surface_alt),
        border: Border {
            color: if focused {
                palette.accent
            } else {
                palette.border
            },
            width: if focused { 1.5 } else { 1.0 },
            radius: 6.0.into(),
        },
        icon: palette.muted,
        placeholder: palette.subtle,
        value: palette.text,
        selection: tint(palette.accent, palette.surface_alt, 0.35),
    }
}

fn panel_style(
    background: Color,
    border: Color,
    radius: f32,
    shadow: bool,
) -> iced::widget::container::Style {
    iced::widget::container::Style {
        text_color: None,
        background: Some(Background::Color(background)),
        border: Border {
            color: border,
            width: if border.a > 0.0 { 1.0 } else { 0.0 },
            radius: radius.into(),
        },
        shadow: if shadow {
            Shadow {
                color: Color {
                    a: 0.18,
                    ..Color::BLACK
                },
                offset: Vector::new(0.0, 1.0),
                blur_radius: 8.0,
            }
        } else {
            Shadow::default()
        },
        snap: false,
    }
}

fn chip_color(palette: Palette, kind: ChipKind) -> Color {
    match kind {
        ChipKind::Neutral => palette.muted,
        ChipKind::Accent => palette.accent,
        ChipKind::Success => palette.success,
        ChipKind::Warning => palette.warning,
        ChipKind::Danger => palette.danger,
    }
}

fn tint(foreground: Color, background: Color, amount: f32) -> Color {
    let amount = amount.clamp(0.0, 1.0);
    Color {
        r: foreground.r * amount + background.r * (1.0 - amount),
        g: foreground.g * amount + background.g * (1.0 - amount),
        b: foreground.b * amount + background.b * (1.0 - amount),
        a: 1.0,
    }
}

fn button_padding() -> Padding {
    Padding {
        top: 8.0,
        right: 12.0,
        bottom: 8.0,
        left: 12.0,
    }
}

fn icon_bytes(icon: Icon) -> &'static [u8] {
    match icon {
        Icon::Brand => include_bytes!("../assets/netix-brand.svg"),
        Icon::Overview => include_bytes!("../assets/icon-overview.svg"),
        Icon::Discover => include_bytes!("../assets/icon-discover.svg"),
        Icon::Points => include_bytes!("../assets/icon-points.svg"),
        Icon::Publish => include_bytes!("../assets/icon-publish.svg"),
        Icon::Settings => include_bytes!("../assets/icon-settings.svg"),
        Icon::Logs => include_bytes!("../assets/icon-logs.svg"),
        Icon::Start => include_bytes!("../assets/icon-start.svg"),
        Icon::Stop => include_bytes!("../assets/icon-stop.svg"),
        Icon::Refresh => include_bytes!("../assets/icon-refresh.svg"),
        Icon::Save => include_bytes!("../assets/icon-save.svg"),
        Icon::Edit => include_bytes!("../assets/icon-edit.svg"),
        Icon::Delete => include_bytes!("../assets/icon-delete.svg"),
    }
}

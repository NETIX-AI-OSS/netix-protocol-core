//! Capability-driven iced GUI for the generic republisher.
//!
//! The UI renders protocol-specific connection and point-addressing controls
//! dynamically from each adapter's [`Capabilities`]/[`FieldSpec`], so adding a
//! protocol never requires touching this file.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use iced::widget::{checkbox, column, container, pick_list, row, scrollable, text, Space};
use iced::{Alignment, Element, Length, Subscription, Task, Theme};

use proto_api::{Capabilities, DiscoveryKind, FieldKind, FieldSpec};

use crate::config::{self, AppConfig, UiTheme};
use crate::log::{LogBuffer, LogLevel};
use crate::model::{
    json_scalar, DiscoveredDevice, DiscoveredPoint, PointConfig, PointIdentity, PointSample,
    PointStatus,
};
use crate::protocol::RepublishRegistry;
use crate::topic::telemetry_topic;
use crate::ui::{self, ButtonKind, ChipKind, Icon, Palette};
use crate::worker::{
    spawn_browse, spawn_discovery, spawn_republisher, RepublisherLifecycle, WorkerChannel,
    WorkerEvent,
};

const LOG_CAPACITY: usize = 500;
const RECENT_SAMPLE_CAPACITY: usize = 200;

pub fn run(build_registry: fn() -> RepublishRegistry) -> iced::Result {
    iced::application(
        move || RepublisherApp::new(build_registry()),
        RepublisherApp::update,
        RepublisherApp::view,
    )
    .title("NETIX Republisher")
    .subscription(RepublisherApp::subscription)
    .theme(RepublisherApp::theme)
    .antialiasing(true)
    .run()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Page {
    Connect,
    Points,
    Republish,
    Settings,
    Logs,
}

#[derive(Debug, Clone)]
pub enum Message {
    SelectPage(Page),
    Tick,
    // Connect
    ProtocolSelected(String),
    ConnFieldChanged(String, String),
    ConnBoolToggled(String, bool),
    Discover,
    BrowseDevice(usize),
    AddBrowsedPoint(usize),
    // Point editor
    PeDeviceKey(String),
    PeAddrField(String, String),
    PeTagPath(String),
    PePollInterval(String),
    PeEnabled(bool),
    SavePoint,
    NewPoint,
    EditPoint(usize),
    DeletePoint(usize),
    TogglePoint(usize, bool),
    // MQTT settings
    MqttHost(String),
    MqttPort(String),
    MqttTls(bool),
    MqttClientId(String),
    MqttTopicPrefix(String),
    MqttHealthTopic(String),
    MqttUsername(String),
    MqttPassword(String),
    MqttCaCert(String),
    MqttClientCert(String),
    MqttClientKey(String),
    MqttRetain(bool),
    MqttRememberSecrets(bool),
    ThemeSelected(UiTheme),
    SaveConfig,
    // Republish
    StartRepublisher,
    StopRepublisher,
}

struct RepublisherApp {
    registry: RepublishRegistry,
    caps: HashMap<String, Capabilities>,
    protocol_ids: Vec<String>,
    config: AppConfig,
    config_path: PathBuf,
    page: Page,
    status: String,

    // discovery / browse
    conn_values: BTreeMap<String, String>,
    devices: Vec<DiscoveredDevice>,
    browsed: Vec<DiscoveredPoint>,

    // point editor
    pe_device_key: String,
    pe_addr_values: BTreeMap<String, String>,
    pe_tag: String,
    pe_poll: String,
    pe_enabled: bool,
    editing_index: Option<usize>,

    // mqtt drafts (numeric buffers)
    mqtt_port_buf: String,

    // republisher runtime
    lifecycle: RepublisherLifecycle,
    stop_flag: Option<Arc<AtomicBool>>,
    recent_samples: VecDeque<PointSample>,
    statuses: HashMap<PointIdentity, PointStatus>,
    published_total: usize,

    channel: WorkerChannel,
    logs: LogBuffer,
}

impl RepublisherApp {
    fn new(registry: RepublishRegistry) -> (Self, Task<Message>) {
        let (mut config, config_path, status) = config::load_or_default();
        let protocol_ids = registry.ids();
        let caps: HashMap<String, Capabilities> = registry
            .capabilities()
            .into_iter()
            .map(|c| (c.id.to_string(), c))
            .collect();
        // Default to the first protocol if none selected.
        if config.protocol.is_empty() {
            if let Some(first) = protocol_ids.first() {
                config.protocol = first.clone();
            }
        }
        let conn_values = connection_strings(&config, &caps);
        let mqtt_port_buf = config.mqtt.port.to_string();

        let mut logs = LogBuffer::new(LOG_CAPACITY);
        logs.push(LogLevel::Info, status.clone());

        let mut app = Self {
            registry,
            caps,
            protocol_ids,
            config,
            config_path,
            page: Page::Connect,
            status,
            conn_values,
            devices: Vec::new(),
            browsed: Vec::new(),
            pe_device_key: String::new(),
            pe_addr_values: BTreeMap::new(),
            pe_tag: String::new(),
            pe_poll: "10".to_string(),
            pe_enabled: true,
            editing_index: None,
            mqtt_port_buf,
            lifecycle: RepublisherLifecycle::Stopped,
            stop_flag: None,
            recent_samples: VecDeque::new(),
            statuses: HashMap::new(),
            published_total: 0,
            channel: WorkerChannel::new(),
            logs,
        };
        app.reset_point_editor();
        (app, Task::none())
    }

    fn active_caps(&self) -> Option<&Capabilities> {
        self.caps.get(&self.config.protocol)
    }

    fn palette(&self) -> Palette {
        ui::palette(self.config.ui.theme)
    }

    fn theme(&self) -> Theme {
        match self.config.ui.theme {
            UiTheme::Light => Theme::Light,
            UiTheme::Auto | UiTheme::Dark => Theme::Dark,
        }
    }

    fn subscription(&self) -> Subscription<Message> {
        iced::time::every(Duration::from_millis(250)).map(|_| Message::Tick)
    }

    fn reset_point_editor(&mut self) {
        self.pe_device_key.clear();
        self.pe_tag.clear();
        self.pe_poll = "10".to_string();
        self.pe_enabled = true;
        self.editing_index = None;
        self.pe_addr_values.clear();
        let specs: Vec<FieldSpec> = self
            .active_caps()
            .map(|c| c.addressing_fields.clone())
            .unwrap_or_default();
        for spec in &specs {
            self.pe_addr_values
                .insert(spec.key.clone(), default_field_string(spec));
        }
    }

    fn save_status(&mut self, level: LogLevel, message: impl Into<String>) {
        let message = message.into();
        self.status = message.clone();
        self.logs.push(level, message);
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::SelectPage(page) => self.page = page,
            Message::Tick => self.drain_worker_events(),
            Message::ProtocolSelected(id) => {
                self.config.protocol = id;
                self.conn_values = connection_strings(&self.config, &self.caps);
                self.devices.clear();
                self.browsed.clear();
                self.reset_point_editor();
            }
            Message::ConnFieldChanged(key, value) => {
                self.conn_values.insert(key, value);
                self.persist_connection();
            }
            Message::ConnBoolToggled(key, value) => {
                self.conn_values.insert(key, value.to_string());
                self.persist_connection();
            }
            Message::Discover => self.start_discovery(),
            Message::BrowseDevice(index) => self.start_browse(index),
            Message::AddBrowsedPoint(index) => self.add_browsed_point(index),
            Message::PeDeviceKey(value) => self.pe_device_key = value,
            Message::PeAddrField(key, value) => {
                self.pe_addr_values.insert(key, value);
            }
            Message::PeTagPath(value) => self.pe_tag = value,
            Message::PePollInterval(value) => self.pe_poll = value,
            Message::PeEnabled(value) => self.pe_enabled = value,
            Message::SavePoint => self.save_point(),
            Message::NewPoint => self.reset_point_editor(),
            Message::EditPoint(index) => self.load_point_into_editor(index),
            Message::DeletePoint(index) => {
                if index < self.config.points.len() {
                    self.config.points.remove(index);
                    self.reset_point_editor();
                    self.save_config();
                }
            }
            Message::TogglePoint(index, enabled) => {
                if let Some(point) = self.config.points.get_mut(index) {
                    point.enabled = enabled;
                    self.save_config();
                }
            }
            Message::MqttHost(v) => self.config.mqtt.host = v,
            Message::MqttPort(v) => {
                self.mqtt_port_buf = v.clone();
                if let Ok(port) = v.trim().parse() {
                    self.config.mqtt.port = port;
                }
            }
            Message::MqttTls(v) => self.config.mqtt.use_tls = v,
            Message::MqttClientId(v) => self.config.mqtt.client_id = v,
            Message::MqttTopicPrefix(v) => self.config.mqtt.topic_prefix = v,
            Message::MqttHealthTopic(v) => self.config.mqtt.health_topic = v,
            Message::MqttUsername(v) => self.config.mqtt.username = non_empty(v),
            Message::MqttPassword(v) => self.config.mqtt.password = non_empty(v),
            Message::MqttCaCert(v) => self.config.mqtt.ca_cert_path = non_empty(v),
            Message::MqttClientCert(v) => self.config.mqtt.client_cert_path = non_empty(v),
            Message::MqttClientKey(v) => self.config.mqtt.client_key_path = non_empty(v),
            Message::MqttRetain(v) => self.config.mqtt.retain = v,
            Message::MqttRememberSecrets(v) => self.config.mqtt.remember_secrets = v,
            Message::ThemeSelected(theme) => {
                self.config.ui.theme = theme;
                self.save_config();
            }
            Message::SaveConfig => self.save_config(),
            Message::StartRepublisher => self.start_republisher(),
            Message::StopRepublisher => self.stop_republisher(),
        }
        Task::none()
    }

    fn persist_connection(&mut self) {
        let Some(caps) = self.caps.get(&self.config.protocol).cloned() else {
            return;
        };
        let addressing = build_addressing(&caps.connection_fields, &self.conn_values);
        *self.config.connection_mut() = addressing;
    }

    fn save_config(&mut self) {
        if let Err(error) = self.config.validate() {
            self.save_status(LogLevel::Error, format!("Config invalid: {error}"));
            return;
        }
        match config::save_to_path(&self.config_path, &self.config) {
            Ok(()) => self.save_status(LogLevel::Info, "Configuration saved"),
            Err(error) => self.save_status(LogLevel::Error, format!("Save failed: {error:#}")),
        }
    }

    fn start_discovery(&mut self) {
        let Some(factory) = self.registry.get(&self.config.protocol) else {
            self.save_status(LogLevel::Error, "No protocol selected");
            return;
        };
        self.persist_connection();
        let conn = self.config.connection();
        self.devices.clear();
        self.save_status(LogLevel::Info, "Discovering…");
        spawn_discovery(self.channel.sender.clone(), factory, conn);
    }

    fn start_browse(&mut self, index: usize) {
        let Some(device) = self.devices.get(index).cloned() else {
            return;
        };
        let Some(factory) = self.registry.get(&self.config.protocol) else {
            return;
        };
        self.persist_connection();
        let conn = self.config.connection();
        self.browsed.clear();
        self.save_status(LogLevel::Info, format!("Browsing {}…", device.key));
        spawn_browse(self.channel.sender.clone(), factory, conn, device);
    }

    fn add_browsed_point(&mut self, index: usize) {
        let Some(found) = self.browsed.get(index).cloned() else {
            return;
        };
        let point = PointConfig {
            enabled: true,
            device_key: found.device_key,
            addressing: found.addressing,
            tag_path: found.suggested_tag_path,
            poll_interval_secs: 10,
        };
        self.upsert_point(point);
        self.save_status(LogLevel::Info, "Added point from browse");
    }

    fn load_point_into_editor(&mut self, index: usize) {
        let Some(point) = self.config.points.get(index).cloned() else {
            return;
        };
        self.pe_device_key = point.device_key.clone();
        self.pe_tag = point.tag_path.clone();
        self.pe_poll = point.poll_interval_secs.to_string();
        self.pe_enabled = point.enabled;
        self.editing_index = Some(index);
        self.pe_addr_values.clear();
        let specs: Vec<FieldSpec> = self
            .active_caps()
            .map(|c| c.addressing_fields.clone())
            .unwrap_or_default();
        for spec in &specs {
            let value = point
                .addressing
                .get(&spec.key)
                .map(json_scalar)
                .unwrap_or_else(|| default_field_string(spec));
            self.pe_addr_values.insert(spec.key.clone(), value);
        }
    }

    fn save_point(&mut self) {
        let Some(caps) = self.caps.get(&self.config.protocol).cloned() else {
            self.save_status(LogLevel::Error, "Select a protocol first");
            return;
        };
        let addressing = build_addressing(&caps.addressing_fields, &self.pe_addr_values);
        let point = PointConfig {
            enabled: self.pe_enabled,
            device_key: self.pe_device_key.trim().to_string(),
            addressing,
            tag_path: self.pe_tag.trim().to_string(),
            poll_interval_secs: self.pe_poll.trim().parse().unwrap_or(10).max(1),
        };
        if let Some(index) = self.editing_index {
            if index < self.config.points.len() {
                self.config.points[index] = point;
            }
        } else {
            self.config.points.push(point);
        }
        self.reset_point_editor();
        self.save_config();
    }

    fn upsert_point(&mut self, point: PointConfig) {
        let id = PointIdentity::from_point(&point);
        if let Some(existing) = self
            .config
            .points
            .iter_mut()
            .find(|p| PointIdentity::from_point(p) == id)
        {
            *existing = point;
        } else {
            self.config.points.push(point);
        }
        self.save_config();
    }

    fn start_republisher(&mut self) {
        if let Err(error) = self.config.validate() {
            self.save_status(LogLevel::Error, format!("Cannot start: {error}"));
            return;
        }
        let Some(factory) = self.registry.get(&self.config.protocol) else {
            self.save_status(LogLevel::Error, "No protocol selected");
            return;
        };
        self.persist_connection();
        let stop = Arc::new(AtomicBool::new(false));
        self.stop_flag = Some(Arc::clone(&stop));
        self.published_total = 0;
        spawn_republisher(
            self.channel.sender.clone(),
            factory,
            self.config.connection(),
            self.config.mqtt.clone(),
            self.config.points.clone(),
            stop,
        );
        self.save_status(LogLevel::Info, "Republisher starting…");
    }

    fn stop_republisher(&mut self) {
        if let Some(stop) = &self.stop_flag {
            stop.store(true, Ordering::Relaxed);
            self.save_status(LogLevel::Info, "Stopping republisher…");
        }
    }

    fn drain_worker_events(&mut self) {
        while let Ok(event) = self.channel.receiver.try_recv() {
            match event {
                WorkerEvent::Log(level, message) => self.logs.push(level, message),
                WorkerEvent::Devices(outcome) => {
                    self.devices = outcome.devices;
                    for warning in outcome.warnings {
                        self.logs.push(LogLevel::Warning, warning);
                    }
                }
                WorkerEvent::Points(points) => self.browsed = points,
                WorkerEvent::Samples(samples) => {
                    for sample in samples {
                        let id = PointIdentity::from_point(&sample.point);
                        self.statuses.entry(id).or_default().record_sample(&sample);
                        self.recent_samples.push_front(sample);
                    }
                    while self.recent_samples.len() > RECENT_SAMPLE_CAPACITY {
                        self.recent_samples.pop_back();
                    }
                }
                WorkerEvent::Failures(failures) => {
                    for failure in failures {
                        let id = PointIdentity::from_point(&failure.point);
                        self.statuses
                            .entry(id)
                            .or_default()
                            .record_read_failure(failure.error);
                    }
                }
                WorkerEvent::PublishStatus(stats) => {
                    self.published_total += stats.published;
                }
                WorkerEvent::Lifecycle(lifecycle) => {
                    if let RepublisherLifecycle::Failed(ref error) = lifecycle {
                        self.logs
                            .push(LogLevel::Error, format!("Republisher failed: {error}"));
                    }
                    self.lifecycle = lifecycle;
                }
                WorkerEvent::Finished(message) => self.logs.push(LogLevel::Info, message),
            }
        }
    }

    // ---- views ----

    fn view(&self) -> Element<'_, Message> {
        let palette = self.palette();
        let sidebar = container(
            column![
                ui::brand(),
                Space::new().height(Length::Fixed(8.0)),
                self.nav(palette, Page::Connect, Icon::Discover, "Connect"),
                self.nav(palette, Page::Points, Icon::Points, "Points"),
                self.nav(palette, Page::Republish, Icon::Publish, "Republish"),
                self.nav(palette, Page::Settings, Icon::Settings, "Settings"),
                self.nav(palette, Page::Logs, Icon::Logs, "Logs"),
            ]
            .spacing(6)
            .padding(16),
        )
        .width(Length::Fixed(220.0))
        .height(Length::Fill)
        .style(move |_| ui::sidebar_style(palette));

        let body = scrollable(
            container(self.page_view(palette))
                .padding(20)
                .width(Length::Fill),
        )
        .height(Length::Fill);

        let status_bar = container(text(self.status.as_str()).size(13).color(palette.muted))
            .padding(8.0)
            .width(Length::Fill)
            .style(move |_| ui::status_bar_style(palette));

        row![sidebar, column![body, status_bar].width(Length::Fill)]
            .height(Length::Fill)
            .into()
    }

    fn nav(&self, palette: Palette, page: Page, icon: Icon, label: &str) -> Element<'_, Message> {
        ui::nav_button(palette, icon, label, self.page == page)
            .on_press(Message::SelectPage(page))
            .into()
    }

    fn page_view(&self, palette: Palette) -> Element<'_, Message> {
        match self.page {
            Page::Connect => self.connect_page(palette),
            Page::Points => self.points_page(palette),
            Page::Republish => self.republish_page(palette),
            Page::Settings => self.settings_page(palette),
            Page::Logs => self.logs_page(palette),
        }
    }

    fn connect_page(&self, palette: Palette) -> Element<'_, Message> {
        let protocol_pick = pick_list(
            self.protocol_ids.clone(),
            (!self.config.protocol.is_empty()).then(|| self.config.protocol.clone()),
            Message::ProtocolSelected,
        )
        .placeholder("Select protocol");

        let mut content = column![
            ui::section_title(palette, "Connection"),
            row![text("Protocol").size(13).color(palette.text), protocol_pick]
                .spacing(12)
                .align_y(Alignment::Center),
        ]
        .spacing(14);

        if let Some(caps) = self.active_caps() {
            let mut fields = column![].spacing(10);
            for spec in &caps.connection_fields {
                fields = fields.push(self.render_field(
                    palette,
                    spec,
                    &self.conn_values,
                    |k, v| Message::ConnFieldChanged(k, v),
                    |k, v| Message::ConnBoolToggled(k, v),
                ));
            }
            content = content.push(ui::card(palette, fields));

            // Discovery / browse
            let discover_label = match caps.discovery {
                DiscoveryKind::Broadcast => "Discover (broadcast)",
                DiscoveryKind::EndpointQuery => "Query endpoints",
                DiscoveryKind::SubnetScan => "Scan subnet",
                DiscoveryKind::ManualOnly => "Discover",
            };
            let discover_btn =
                ui::action_button(palette, Icon::Refresh, discover_label, ButtonKind::Primary)
                    .on_press(Message::Discover);
            content = content.push(discover_btn);

            if !self.devices.is_empty() {
                let mut list = column![ui::eyebrow(palette, "DISCOVERED DEVICES")].spacing(8);
                for (index, device) in self.devices.iter().enumerate() {
                    let browse =
                        ui::action_button(palette, Icon::Discover, "Browse", ButtonKind::Secondary)
                            .on_press(Message::BrowseDevice(index));
                    list = list.push(ui::card(
                        palette,
                        row![
                            column![
                                text(device.key.as_str()).size(14).color(palette.text),
                                text(format!("{} — {}", device.address, device.detail))
                                    .size(12)
                                    .color(palette.muted),
                            ]
                            .spacing(2)
                            .width(Length::Fill),
                            browse,
                        ]
                        .align_y(Alignment::Center),
                    ));
                }
                content = content.push(list);
            }

            if !self.browsed.is_empty() {
                let mut list = column![ui::eyebrow(palette, "BROWSED POINTS")].spacing(6);
                for (index, point) in self.browsed.iter().enumerate() {
                    let add = ui::action_button(palette, Icon::Save, "Add", ButtonKind::Secondary)
                        .on_press(Message::AddBrowsedPoint(index));
                    let name = point
                        .name
                        .clone()
                        .unwrap_or_else(|| point.suggested_tag_path.clone());
                    let value = point
                        .value
                        .as_ref()
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "—".into());
                    list = list.push(ui::card(
                        palette,
                        row![
                            column![
                                text(name).size(13).color(palette.text),
                                text(format!(
                                    "{}  =  {value}",
                                    addressing_summary(&point.addressing)
                                ))
                                .size(12)
                                .color(palette.muted),
                            ]
                            .spacing(2)
                            .width(Length::Fill),
                            add,
                        ]
                        .align_y(Alignment::Center),
                    ));
                }
                content = content.push(list);
            }
        } else {
            content = content.push(ui::muted(palette, "No protocols are registered."));
        }

        content.into()
    }

    fn points_page(&self, palette: Palette) -> Element<'_, Message> {
        // Editor
        let mut editor = column![
            ui::section_title(
                palette,
                if self.editing_index.is_some() {
                    "Edit point"
                } else {
                    "New point"
                }
            ),
            ui::labeled_input(
                palette,
                "Device key",
                "label",
                &self.pe_device_key,
                Message::PeDeviceKey
            ),
        ]
        .spacing(10);

        if let Some(caps) = self.active_caps() {
            for spec in &caps.addressing_fields {
                editor = editor.push(self.render_field(
                    palette,
                    spec,
                    &self.pe_addr_values,
                    |k, v| Message::PeAddrField(k, v),
                    |_k, _v| Message::NewPoint, // addressing has no bool fields today
                ));
            }
        }
        editor = editor
            .push(ui::labeled_input(
                palette,
                "Tag path",
                "optional",
                &self.pe_tag,
                Message::PeTagPath,
            ))
            .push(ui::labeled_input(
                palette,
                "Poll interval (s)",
                "",
                &self.pe_poll,
                Message::PePollInterval,
            ))
            .push(
                checkbox(self.pe_enabled)
                    .label("Enabled")
                    .on_toggle(Message::PeEnabled),
            )
            .push(
                row![
                    ui::action_button(palette, Icon::Save, "Save point", ButtonKind::Primary)
                        .on_press(Message::SavePoint),
                    ui::action_button(palette, Icon::Edit, "New", ButtonKind::Secondary)
                        .on_press(Message::NewPoint),
                ]
                .spacing(10),
            );

        // List
        let mut list = column![ui::section_title(
            palette,
            format!("Configured points ({})", self.config.points.len())
        )]
        .spacing(6);
        for (index, point) in self.config.points.iter().enumerate() {
            let topic = telemetry_topic(&self.config.mqtt, point);
            let toggle = checkbox(point.enabled).on_toggle(move |v| Message::TogglePoint(index, v));
            list = list.push(ui::card(
                palette,
                row![
                    toggle,
                    column![
                        text(point.display_name()).size(13).color(palette.text),
                        text(topic).size(11).color(palette.subtle),
                    ]
                    .spacing(2)
                    .width(Length::Fill),
                    ui::action_button(palette, Icon::Edit, "Edit", ButtonKind::Ghost)
                        .on_press(Message::EditPoint(index)),
                    ui::action_button(palette, Icon::Delete, "Delete", ButtonKind::Danger)
                        .on_press(Message::DeletePoint(index)),
                ]
                .spacing(10)
                .align_y(Alignment::Center),
            ));
        }

        row![
            editor.width(Length::FillPortion(2)),
            list.width(Length::FillPortion(3))
        ]
        .spacing(20)
        .into()
    }

    fn republish_page(&self, palette: Palette) -> Element<'_, Message> {
        let (state_label, chip_kind) = match &self.lifecycle {
            RepublisherLifecycle::Running => ("Running", ChipKind::Success),
            RepublisherLifecycle::Starting => ("Starting", ChipKind::Warning),
            RepublisherLifecycle::Stopped => ("Stopped", ChipKind::Neutral),
            RepublisherLifecycle::Failed(_) => ("Failed", ChipKind::Danger),
        };
        let running = matches!(
            self.lifecycle,
            RepublisherLifecycle::Running | RepublisherLifecycle::Starting
        );
        let control = if running {
            ui::action_button(palette, Icon::Stop, "Stop", ButtonKind::Danger)
                .on_press(Message::StopRepublisher)
        } else {
            ui::action_button(palette, Icon::Start, "Start", ButtonKind::Primary)
                .on_press(Message::StartRepublisher)
        };

        let metrics = row![
            ui::metric(
                palette,
                "State",
                state_label,
                &self.config.protocol,
                chip_kind
            ),
            ui::metric(
                palette,
                "Published",
                self.published_total.to_string(),
                "samples",
                ChipKind::Accent
            ),
            ui::metric(
                palette,
                "Points",
                self.config.points.len().to_string(),
                "configured",
                ChipKind::Neutral
            ),
        ]
        .spacing(14);

        let mut samples = column![ui::eyebrow(palette, "RECENT SAMPLES")].spacing(4);
        for sample in self.recent_samples.iter().take(40) {
            samples = samples.push(
                row![
                    text(sample.topic.as_str())
                        .size(12)
                        .color(palette.muted)
                        .width(Length::Fill),
                    text(sample.value.to_string()).size(12).color(palette.text),
                ]
                .spacing(10),
            );
        }

        column![
            row![
                ui::section_title(palette, "Republish"),
                Space::new().width(Length::Fill),
                control
            ]
            .align_y(Alignment::Center),
            metrics,
            ui::card(palette, samples),
        ]
        .spacing(16)
        .into()
    }

    fn settings_page(&self, palette: Palette) -> Element<'_, Message> {
        let mqtt = &self.config.mqtt;
        let fields = column![
            ui::labeled_input(palette, "MQTT host", "", &mqtt.host, Message::MqttHost),
            ui::labeled_input(
                palette,
                "MQTT port",
                "",
                &self.mqtt_port_buf,
                Message::MqttPort
            ),
            checkbox(mqtt.use_tls)
                .label("Use TLS")
                .on_toggle(Message::MqttTls),
            ui::labeled_input(
                palette,
                "Client ID",
                "",
                &mqtt.client_id,
                Message::MqttClientId
            ),
            ui::labeled_input(
                palette,
                "Topic prefix",
                "",
                &mqtt.topic_prefix,
                Message::MqttTopicPrefix
            ),
            ui::labeled_input(
                palette,
                "Health topic",
                "",
                &mqtt.health_topic,
                Message::MqttHealthTopic
            ),
            ui::labeled_input(
                palette,
                "Username",
                "optional",
                mqtt.username.as_deref().unwrap_or(""),
                Message::MqttUsername
            ),
            ui::labeled_input(
                palette,
                "Password",
                "optional",
                mqtt.password.as_deref().unwrap_or(""),
                Message::MqttPassword
            ),
            ui::labeled_input(
                palette,
                "CA cert path",
                "optional",
                mqtt.ca_cert_path.as_deref().unwrap_or(""),
                Message::MqttCaCert
            ),
            ui::labeled_input(
                palette,
                "Client cert path",
                "optional",
                mqtt.client_cert_path.as_deref().unwrap_or(""),
                Message::MqttClientCert
            ),
            ui::labeled_input(
                palette,
                "Client key path",
                "optional",
                mqtt.client_key_path.as_deref().unwrap_or(""),
                Message::MqttClientKey
            ),
            checkbox(mqtt.retain)
                .label("Retain")
                .on_toggle(Message::MqttRetain),
            checkbox(mqtt.remember_secrets)
                .label("Remember secrets in config")
                .on_toggle(Message::MqttRememberSecrets),
            row![
                text("Theme").size(13).color(palette.text),
                pick_list(
                    UiTheme::ALL.to_vec(),
                    Some(self.config.ui.theme),
                    Message::ThemeSelected
                ),
            ]
            .spacing(12)
            .align_y(Alignment::Center),
        ]
        .spacing(10);

        column![
            ui::section_title(palette, "MQTT / TLS"),
            ui::card(palette, fields),
            ui::action_button(
                palette,
                Icon::Save,
                "Save configuration",
                ButtonKind::Primary
            )
            .on_press(Message::SaveConfig),
        ]
        .spacing(16)
        .into()
    }

    fn logs_page(&self, palette: Palette) -> Element<'_, Message> {
        let mut lines = column![].spacing(2);
        for entry in self.logs.entries().iter().rev().take(200) {
            let color = match entry.level {
                LogLevel::Info => palette.muted,
                LogLevel::Warning => palette.warning,
                LogLevel::Error => palette.danger,
            };
            lines = lines.push(
                text(format!("[{}] {}", entry.level, entry.message))
                    .size(12)
                    .color(color),
            );
        }
        column![ui::section_title(palette, "Logs"), ui::card(palette, lines)]
            .spacing(16)
            .into()
    }

    /// Render one capability field as the appropriate widget.
    fn render_field<'a>(
        &'a self,
        palette: Palette,
        spec: &'a FieldSpec,
        values: &'a BTreeMap<String, String>,
        on_text: impl Fn(String, String) -> Message + 'a,
        on_bool: impl Fn(String, bool) -> Message + 'a,
    ) -> Element<'a, Message> {
        let current: &str = values.get(&spec.key).map(String::as_str).unwrap_or("");
        match &spec.kind {
            FieldKind::Bool => {
                let key = spec.key.clone();
                checkbox(current == "true")
                    .label(spec.label.clone())
                    .on_toggle(move |v| on_bool(key.clone(), v))
                    .into()
            }
            FieldKind::Enum(options) => {
                let key = spec.key.clone();
                let selected = (!current.is_empty()).then(|| current.to_string());
                row![
                    text(spec.label.clone())
                        .size(13)
                        .color(palette.text)
                        .width(Length::Fixed(160.0)),
                    pick_list(options.clone(), selected, move |v| on_text(key.clone(), v)),
                ]
                .spacing(10)
                .align_y(Alignment::Center)
                .into()
            }
            FieldKind::Text | FieldKind::U32 => {
                let key = spec.key.clone();
                let hint = spec.help.as_deref().unwrap_or("");
                ui::labeled_input(palette, &spec.label, hint, current, move |v| {
                    on_text(key.clone(), v)
                })
            }
        }
    }
}

// ---- helpers ----

fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn default_field_string(spec: &FieldSpec) -> String {
    spec.default.as_ref().map(json_scalar).unwrap_or_default()
}

fn addressing_summary(addressing: &proto_api::Addressing) -> String {
    addressing
        .iter()
        .map(|(k, v)| format!("{k}={}", json_scalar(v)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn connection_strings(
    config: &AppConfig,
    caps: &HashMap<String, Capabilities>,
) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    if let Some(caps) = caps.get(&config.protocol) {
        let existing = config.connection();
        for spec in &caps.connection_fields {
            let value = existing
                .get(&spec.key)
                .map(json_scalar)
                .unwrap_or_else(|| default_field_string(spec));
            values.insert(spec.key.clone(), value);
        }
    }
    values
}

fn build_addressing(
    specs: &[FieldSpec],
    values: &BTreeMap<String, String>,
) -> proto_api::Addressing {
    let mut addressing = proto_api::Addressing::new();
    for spec in specs {
        let raw = values.get(&spec.key).cloned().unwrap_or_default();
        let value = match &spec.kind {
            FieldKind::U32 => raw
                .trim()
                .parse::<u64>()
                .map(|n| serde_json::json!(n))
                .unwrap_or_else(|_| serde_json::json!(raw)),
            FieldKind::Bool => serde_json::json!(raw == "true"),
            _ => serde_json::json!(raw),
        };
        addressing.insert(spec.key.clone(), value);
    }
    addressing
}

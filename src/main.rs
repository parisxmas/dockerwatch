use std::{
    io,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use bollard::{
    container::{InspectContainerOptions, KillContainerOptions, ListContainersOptions, StatsOptions},
    models::ContainerInspectResponse,
    Docker,
};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::{stream::FuturesUnordered, StreamExt};
use humansize::{format_size, BINARY};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Gauge, Paragraph, Row, Table, TableState, Wrap},
    Frame, Terminal,
};
use tokio::sync::watch;

#[derive(Clone, Debug, Default)]
struct ContainerRow {
    id: String,
    name: String,
    image: String,
    status: String,
    cpu_pct: f64,
    cores: f64,
    mem_usage: u64,
    mem_limit: u64,
    mem_pct: f64,
}

#[derive(Clone, Debug, Default)]
struct Snapshot {
    rows: Vec<ContainerRow>,
    host_mem_total: u64,
    host_ncpu: u64,
    error: Option<String>,
    fetched_at: Option<Instant>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Sort {
    Cpu,
    Mem,
    Name,
}

impl Sort {
    fn next(self) -> Self {
        match self {
            Sort::Cpu => Sort::Mem,
            Sort::Mem => Sort::Name,
            Sort::Name => Sort::Cpu,
        }
    }
    fn label(self) -> &'static str {
        match self {
            Sort::Cpu => "CPU%",
            Sort::Mem => "MEM%",
            Sort::Name => "NAME",
        }
    }
}

enum View {
    Table,
    Detail(DetailState),
}

struct DetailState {
    id: String,
    name: String,
    inspect: Option<ContainerInspectResponse>,
    confirming_kill: bool,
    status_msg: Option<String>,
    is_error: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let docker = Docker::connect_with_local_defaults()
        .context("failed to connect to Docker daemon (is it running?)")?;
    docker
        .ping()
        .await
        .context("Docker daemon did not respond to ping")?;

    let (tx, rx) = watch::channel(Snapshot::default());
    let docker_clone = docker.clone();
    tokio::spawn(async move {
        poll_loop(docker_clone, tx).await;
    });

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let res = run_ui(&mut terminal, rx, docker.clone()).await;

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    res
}

async fn poll_loop(docker: Docker, tx: watch::Sender<Snapshot>) {
    let interval = Duration::from_millis(1000);
    loop {
        let started = Instant::now();
        let snapshot = match fetch_snapshot(&docker).await {
            Ok((rows, host_mem_total, host_ncpu)) => Snapshot {
                rows,
                host_mem_total,
                host_ncpu,
                error: None,
                fetched_at: Some(Instant::now()),
            },
            Err(e) => Snapshot {
                rows: Vec::new(),
                host_mem_total: 0,
                host_ncpu: 0,
                error: Some(format!("{e:#}")),
                fetched_at: Some(Instant::now()),
            },
        };
        if tx.send(snapshot).is_err() {
            return;
        }
        let elapsed = started.elapsed();
        if elapsed < interval {
            tokio::time::sleep(interval - elapsed).await;
        }
    }
}

async fn fetch_snapshot(docker: &Docker) -> Result<(Vec<ContainerRow>, u64, u64)> {
    let info = docker.info().await?;
    let host_mem_total = info.mem_total.unwrap_or(0).max(0) as u64;
    let host_ncpu = info.ncpu.unwrap_or(0).max(0) as u64;

    let containers = docker
        .list_containers(Some(ListContainersOptions::<String> {
            all: false,
            ..Default::default()
        }))
        .await?;

    let mut futs = FuturesUnordered::new();
    for c in containers {
        let Some(id) = c.id.clone() else { continue };
        let name = c
            .names
            .as_ref()
            .and_then(|n| n.first())
            .map(|n| n.trim_start_matches('/').to_string())
            .unwrap_or_else(|| id[..12].to_string());
        let image = c.image.clone().unwrap_or_default();
        let status = c.status.clone().unwrap_or_default();
        let docker = docker.clone();
        futs.push(async move {
            let stats_res = docker
                .stats(
                    &id,
                    Some(StatsOptions {
                        stream: false,
                        one_shot: false,
                    }),
                )
                .next()
                .await;
            (id, name, image, status, stats_res)
        });
    }

    let mut rows = Vec::new();
    while let Some((id, name, image, status, stats_res)) = futs.next().await {
        let Some(Ok(stats)) = stats_res else { continue };

        let cpu_pct = calc_cpu_pct(&stats);
        let (mem_usage, mem_limit) = calc_mem(&stats);
        let mem_pct = if mem_limit > 0 {
            (mem_usage as f64 / mem_limit as f64) * 100.0
        } else {
            0.0
        };

        let cores = cpu_pct / 100.0 * host_ncpu as f64;
        rows.push(ContainerRow {
            id: id.chars().take(12).collect(),
            name,
            image,
            status,
            cpu_pct,
            cores,
            mem_usage,
            mem_limit,
            mem_pct,
        });
    }

    Ok((rows, host_mem_total, host_ncpu))
}

// Per-container CPU as a percentage of the WHOLE host's CPU capacity.
// 100% means the container is saturating every core on the host.
fn calc_cpu_pct(stats: &bollard::container::Stats) -> f64 {
    let cpu_delta = stats.cpu_stats.cpu_usage.total_usage as f64
        - stats.precpu_stats.cpu_usage.total_usage as f64;
    let sys_delta = stats
        .cpu_stats
        .system_cpu_usage
        .unwrap_or(0)
        .saturating_sub(stats.precpu_stats.system_cpu_usage.unwrap_or(0))
        as f64;
    if sys_delta > 0.0 && cpu_delta > 0.0 {
        (cpu_delta / sys_delta) * 100.0
    } else {
        0.0
    }
}

fn calc_mem(stats: &bollard::container::Stats) -> (u64, u64) {
    let usage = stats.memory_stats.usage.unwrap_or(0);
    // Docker CLI subtracts cache from usage where available.
    let cache = stats
        .memory_stats
        .stats
        .as_ref()
        .and_then(|s| match s {
            bollard::container::MemoryStatsStats::V1(v1) => Some(v1.cache),
            bollard::container::MemoryStatsStats::V2(v2) => Some(v2.inactive_file),
        })
        .unwrap_or(0);
    let effective = usage.saturating_sub(cache);
    let limit = stats.memory_stats.limit.unwrap_or(0);
    (effective, limit)
}

async fn run_ui<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    mut rx: watch::Receiver<Snapshot>,
    docker: Docker,
) -> Result<()> {
    let mut table_state = TableState::default();
    table_state.select(Some(0));
    let mut sort = Sort::Cpu;
    let mut filter = String::new();
    let mut editing_filter = false;
    let mut view = View::Table;
    let tick = Duration::from_millis(100);

    loop {
        let snapshot = rx.borrow().clone();
        let mut rows = snapshot.rows.clone();
        rows = apply_filter(rows, &filter);
        sort_rows(&mut rows, sort);

        if !rows.is_empty() {
            let sel = table_state.selected().unwrap_or(0).min(rows.len() - 1);
            table_state.select(Some(sel));
        } else {
            table_state.select(None);
        }

        terminal.draw(|f| {
            draw(
                f,
                &rows,
                &snapshot,
                &mut table_state,
                sort,
                &filter,
                editing_filter,
                &view,
            )
        })?;

        if event::poll(tick)? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }

                match &mut view {
                    View::Detail(d) => match key.code {
                        KeyCode::Esc => {
                            if d.confirming_kill {
                                d.confirming_kill = false;
                            } else {
                                view = View::Table;
                            }
                        }
                        KeyCode::Char('q') if !d.confirming_kill => view = View::Table,
                        KeyCode::Char('k') if !d.confirming_kill => d.confirming_kill = true,
                        KeyCode::Char('y') | KeyCode::Enter if d.confirming_kill => {
                            let id = d.id.clone();
                            match docker
                                .kill_container(
                                    &id,
                                    Some(KillContainerOptions { signal: "SIGKILL" }),
                                )
                                .await
                            {
                                Ok(()) => {
                                    d.status_msg = Some(format!("killed {}", short_id(&id)));
                                    d.is_error = false;
                                }
                                Err(e) => {
                                    d.status_msg = Some(format!("kill failed: {e}"));
                                    d.is_error = true;
                                }
                            }
                            d.confirming_kill = false;
                        }
                        KeyCode::Char('n') if d.confirming_kill => d.confirming_kill = false,
                        _ => {}
                    },
                    View::Table => {
                        if editing_filter {
                            match key.code {
                                KeyCode::Esc => {
                                    filter.clear();
                                    editing_filter = false;
                                }
                                KeyCode::Enter => editing_filter = false,
                                KeyCode::Backspace => {
                                    filter.pop();
                                }
                                KeyCode::Char(c) => filter.push(c),
                                _ => {}
                            }
                        } else {
                            match key.code {
                                KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                                KeyCode::Down | KeyCode::Char('j') => {
                                    if !rows.is_empty() {
                                        let i = table_state.selected().unwrap_or(0);
                                        table_state
                                            .select(Some((i + 1).min(rows.len() - 1)));
                                    }
                                }
                                KeyCode::Up | KeyCode::Char('k') => {
                                    if !rows.is_empty() {
                                        let i = table_state.selected().unwrap_or(0);
                                        table_state.select(Some(i.saturating_sub(1)));
                                    }
                                }
                                KeyCode::Char('s') => sort = sort.next(),
                                KeyCode::Char('c') => sort = Sort::Cpu,
                                KeyCode::Char('m') => sort = Sort::Mem,
                                KeyCode::Char('n') => sort = Sort::Name,
                                KeyCode::Char('/') => editing_filter = true,
                                KeyCode::Enter => {
                                    if let Some(idx) = table_state.selected() {
                                        if let Some(row) = rows.get(idx) {
                                            let mut d = DetailState {
                                                id: row.id.clone(),
                                                name: row.name.clone(),
                                                inspect: None,
                                                confirming_kill: false,
                                                status_msg: None,
                                                is_error: false,
                                            };
                                            match docker
                                                .inspect_container(
                                                    &row.id,
                                                    None::<InspectContainerOptions>,
                                                )
                                                .await
                                            {
                                                Ok(i) => d.inspect = Some(i),
                                                Err(e) => {
                                                    d.status_msg =
                                                        Some(format!("inspect failed: {e}"));
                                                    d.is_error = true;
                                                }
                                            }
                                            view = View::Detail(d);
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        } else {
            let _ = tokio::time::timeout(Duration::from_millis(50), rx.changed()).await;
        }
    }
}

fn short_id(id: &str) -> &str {
    if id.len() > 12 {
        &id[..12]
    } else {
        id
    }
}

fn apply_filter(rows: Vec<ContainerRow>, filter: &str) -> Vec<ContainerRow> {
    if filter.is_empty() {
        return rows;
    }
    let f = filter.to_lowercase();
    rows.into_iter()
        .filter(|r| r.name.to_lowercase().contains(&f) || r.image.to_lowercase().contains(&f))
        .collect()
}

fn sort_rows(rows: &mut [ContainerRow], sort: Sort) {
    match sort {
        Sort::Cpu => rows.sort_by(|a, b| b.cpu_pct.partial_cmp(&a.cpu_pct).unwrap()),
        Sort::Mem => rows.sort_by(|a, b| b.mem_pct.partial_cmp(&a.mem_pct).unwrap()),
        Sort::Name => rows.sort_by(|a, b| a.name.cmp(&b.name)),
    }
}

fn draw(
    f: &mut Frame,
    rows: &[ContainerRow],
    snapshot: &Snapshot,
    table_state: &mut TableState,
    sort: Sort,
    filter: &str,
    editing_filter: bool,
    view: &View,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(5),
            Constraint::Length(5),
            Constraint::Length(1),
        ])
        .split(f.area());

    draw_header(f, chunks[0], snapshot, rows, sort, filter, editing_filter);
    draw_table(f, chunks[1], rows, table_state, sort);
    draw_detail(f, chunks[2], rows, table_state, snapshot);
    draw_footer(f, chunks[3], filter, editing_filter, view);

    if let View::Detail(d) = view {
        draw_popup(f, d, rows, snapshot);
    }
}

fn centered_fixed(width: u16, height: u16, r: Rect) -> Rect {
    let w = width.min(r.width);
    let h = height.min(r.height);
    let x = r.x + (r.width.saturating_sub(w)) / 2;
    let y = r.y + (r.height.saturating_sub(h)) / 2;
    Rect { x, y, width: w, height: h }
}

fn draw_popup(f: &mut Frame, d: &DetailState, rows: &[ContainerRow], snapshot: &Snapshot) {
    // Build content first so we can size the popup to fit.
    let live = rows.iter().find(|r| r.id == d.id);
    let info_lines = build_detail_lines(d, live, snapshot);
    let has_status = d.status_msg.is_some();

    // height = borders(2) + info + (status line ? 1 : 0) + hints(1)
    let needed_h = (info_lines.len() as u16) + 2 + if has_status { 1 } else { 0 } + 1;
    // width: enough for the longest reasonable label/value pair; cap to frame.
    let max_w = f.area().width.saturating_sub(4).min(110);
    let width = max_w.max(60).min(f.area().width);
    let height = needed_h.min(f.area().height);

    let area = centered_fixed(width, height, f.area());
    f.render_widget(Clear, area);

    let title = format!(" container · {} ", d.name);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let body_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(info_lines.len() as u16),
            Constraint::Length(if has_status { 1 } else { 0 }),
            Constraint::Length(1),
        ])
        .split(inner);

    let info = Paragraph::new(info_lines).wrap(Wrap { trim: false });
    f.render_widget(info, body_layout[0]);

    // status line
    if let Some(msg) = &d.status_msg {
        let color = if d.is_error { Color::Red } else { Color::Green };
        f.render_widget(
            Paragraph::new(Line::from(Span::styled(
                msg.clone(),
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ))),
            body_layout[1],
        );
    }

    // hints / confirm prompt
    let hints = if d.confirming_kill {
        Line::from(vec![
            Span::styled(
                " KILL? ",
                Style::default()
                    .bg(Color::Red)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!(" send SIGKILL to {}?   ", d.name)),
            Span::styled(" y/Enter ", Style::default().fg(Color::Red)),
            Span::raw("confirm  "),
            Span::styled(" n/Esc ", Style::default().fg(Color::Cyan)),
            Span::raw("cancel"),
        ])
    } else {
        Line::from(vec![
            Span::styled(" k ", Style::default().bg(Color::Red).fg(Color::White)),
            Span::raw(" kill   "),
            Span::styled(" Esc/q ", Style::default().fg(Color::Cyan)),
            Span::raw("close"),
        ])
    };
    f.render_widget(Paragraph::new(hints), body_layout[2]);
}

fn field(label: &str, value: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("{label:>11}: "),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw(value.to_string()),
    ])
}

fn build_detail_lines(
    d: &DetailState,
    live: Option<&ContainerRow>,
    snapshot: &Snapshot,
) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();
    lines.push(field("id", &d.id));
    lines.push(field("name", &d.name));
    if let Some(i) = &d.inspect {
        if let Some(img) = &i.image {
            lines.push(field("image", img));
        }
        if let Some(cfg) = &i.config {
            if let Some(image_tag) = &cfg.image {
                lines.push(field("tag", image_tag));
            }
            if let Some(cmd) = &cfg.cmd {
                lines.push(field("cmd", &cmd.join(" ")));
            }
            if let Some(entry) = &cfg.entrypoint {
                lines.push(field("entrypoint", &entry.join(" ")));
            }
            if let Some(wd) = &cfg.working_dir {
                if !wd.is_empty() {
                    lines.push(field("workdir", wd));
                }
            }
        }
        if let Some(state) = &i.state {
            if let Some(s) = &state.status {
                lines.push(field("state", &format!("{s:?}")));
            }
            if let Some(p) = state.pid {
                if p > 0 {
                    lines.push(field("pid", &p.to_string()));
                }
            }
            if let Some(started) = &state.started_at {
                if !started.is_empty() {
                    lines.push(field("started", started));
                }
            }
        }
        if let Some(created) = &i.created {
            lines.push(field("created", created));
        }
        if let Some(net) = &i.network_settings {
            if let Some(networks) = &net.networks {
                let nets: Vec<String> = networks
                    .iter()
                    .map(|(n, ns)| {
                        let ip = ns.ip_address.clone().unwrap_or_default();
                        if ip.is_empty() {
                            n.clone()
                        } else {
                            format!("{n}={ip}")
                        }
                    })
                    .collect();
                if !nets.is_empty() {
                    lines.push(field("networks", &nets.join(", ")));
                }
            }
            if let Some(ports) = &net.ports {
                let mut bindings: Vec<String> = Vec::new();
                for (proto_port, opt_binds) in ports {
                    if let Some(binds) = opt_binds {
                        for b in binds {
                            let host = b
                                .host_port
                                .clone()
                                .filter(|p| !p.is_empty())
                                .unwrap_or_else(|| "?".into());
                            bindings.push(format!("{host}→{proto_port}"));
                        }
                    }
                }
                if !bindings.is_empty() {
                    lines.push(field("ports", &bindings.join(", ")));
                }
            }
        }
        if let Some(host) = &i.host_config {
            if let Some(rp) = &host.restart_policy {
                if let Some(name) = &rp.name {
                    lines.push(field("restart", &format!("{name:?}")));
                }
            }
        }
    } else if !d.is_error {
        lines.push(Line::from(Span::styled(
            "loading inspect...",
            Style::default().fg(Color::DarkGray),
        )));
    }

    if let Some(r) = live {
        let cores_str = if snapshot.host_ncpu > 0 {
            format!("  ({:.3} / {} cores)", r.cores, snapshot.host_ncpu)
        } else {
            String::new()
        };
        lines.push(Line::from(vec![
            Span::styled(
                "       live: ",
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(
                format!("CPU {:.2}%", r.cpu_pct),
                Style::default()
                    .fg(severity_color(r.cpu_pct))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(cores_str),
            Span::raw("    "),
            Span::styled(
                format!(
                    "MEM {} / {} ({:.2}%)",
                    format_size(r.mem_usage, BINARY),
                    format_size(r.mem_limit, BINARY),
                    r.mem_pct
                ),
                Style::default()
                    .fg(severity_color(r.mem_pct))
                    .add_modifier(Modifier::BOLD),
            ),
        ]));
    }
    lines
}

fn draw_header(
    f: &mut Frame,
    area: Rect,
    snapshot: &Snapshot,
    rows: &[ContainerRow],
    sort: Sort,
    filter: &str,
    editing_filter: bool,
) {
    let count = rows.len();
    // Each row.cpu_pct is now already % of the whole host, so the sum is too.
    let total_cpu: f64 = rows.iter().map(|r| r.cpu_pct).sum();
    let total_mem: u64 = rows.iter().map(|r| r.mem_usage).sum();
    let cores_in_use = total_cpu / 100.0 * snapshot.host_ncpu as f64;
    let mem_pct = if snapshot.host_mem_total > 0 {
        (total_mem as f64 / snapshot.host_mem_total as f64) * 100.0
    } else {
        0.0
    };

    let cpu_str = if snapshot.host_ncpu > 0 {
        format!(
            "CPU {:.1}% ({:.2} / {} cores)",
            total_cpu, cores_in_use, snapshot.host_ncpu
        )
    } else {
        format!("CPU {:.1}%", total_cpu)
    };
    let mem_str = if snapshot.host_mem_total > 0 {
        format!(
            "MEM {} / {} ({:.1}%)",
            format_size(total_mem, BINARY),
            format_size(snapshot.host_mem_total, BINARY),
            mem_pct
        )
    } else {
        format!("MEM {}", format_size(total_mem, BINARY))
    };

    let total_count = snapshot.rows.len();
    let count_str = if !filter.is_empty() && count != total_count {
        format!("  containers: {count}/{total_count}")
    } else {
        format!("  containers: {count}")
    };
    let mut spans = vec![
        Span::styled(
            " dockerwatch ",
            Style::default()
                .bg(Color::Cyan)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(count_str),
        Span::raw("    "),
        Span::styled(cpu_str, Style::default().fg(severity_color(total_cpu))),
        Span::raw("    "),
        Span::styled(mem_str, Style::default().fg(severity_color(mem_pct))),
        Span::raw(format!("    sort: {}", sort.label())),
    ];
    if !filter.is_empty() && !editing_filter {
        spans.push(Span::raw("    "));
        spans.push(Span::styled(
            format!("filter: /{filter}"),
            Style::default().fg(Color::Yellow),
        ));
    }
    if let Some(err) = &snapshot.error {
        spans.push(Span::raw("    "));
        spans.push(Span::styled(
            format!("error: {err}"),
            Style::default().fg(Color::Red),
        ));
    } else if let Some(t) = snapshot.fetched_at {
        spans.push(Span::raw(format!(
            "    updated {}ms ago",
            t.elapsed().as_millis()
        )));
    }
    f.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn draw_table(
    f: &mut Frame,
    area: Rect,
    rows: &[ContainerRow],
    state: &mut TableState,
    sort: Sort,
) {
    let arrow = |s: Sort| if sort == s { " ▼" } else { "" };
    let header = Row::new(vec![
        Cell::from("ID"),
        Cell::from(format!("NAME{}", arrow(Sort::Name))),
        Cell::from("IMAGE"),
        Cell::from("STATUS"),
        Cell::from(format!("CPU%{}", arrow(Sort::Cpu)))
            .style(Style::default().fg(Color::Yellow)),
        Cell::from("CORES").style(Style::default().fg(Color::Yellow)),
        Cell::from("MEM USAGE / LIMIT"),
        Cell::from(format!("MEM%{}", arrow(Sort::Mem)))
            .style(Style::default().fg(Color::Magenta)),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let body: Vec<Row> = rows
        .iter()
        .map(|r| {
            let mem = format!(
                "{} / {}",
                format_size(r.mem_usage, BINARY),
                format_size(r.mem_limit, BINARY)
            );
            Row::new(vec![
                Cell::from(r.id.clone()),
                Cell::from(r.name.clone()).style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from(r.image.clone()).style(Style::default().fg(Color::DarkGray)),
                Cell::from(r.status.clone()),
                Cell::from(format!("{:>6.2}", r.cpu_pct))
                    .style(Style::default().fg(severity_color(r.cpu_pct))),
                Cell::from(format!("{:>6.3}", r.cores))
                    .style(Style::default().fg(severity_color(r.cpu_pct))),
                Cell::from(mem),
                Cell::from(format!("{:>6.2}", r.mem_pct))
                    .style(Style::default().fg(severity_color(r.mem_pct))),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(12),
        Constraint::Percentage(20),
        Constraint::Percentage(26),
        Constraint::Percentage(18),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(22),
        Constraint::Length(8),
    ];

    let table = Table::new(body, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" containers "))
        .row_highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▶ ");

    f.render_stateful_widget(table, area, state);
}

fn draw_detail(f: &mut Frame, area: Rect, rows: &[ContainerRow], state: &TableState, snapshot: &Snapshot) {
    let block = Block::default().borders(Borders::ALL).title(" selected ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let Some(idx) = state.selected() else {
        f.render_widget(
            Paragraph::new("(no containers running)").dim(),
            inner,
        );
        return;
    };
    let Some(r) = rows.get(idx) else { return };

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(inner);

    // Each column: header line, value line, gauge.
    let make_col = |c: Rect| {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(1),
            ])
            .split(c)
    };
    let cpu_rows = make_col(cols[0]);
    let mem_rows = make_col(cols[1]);

    // CPU side
    let cpu_label = Paragraph::new(Line::from(vec![
        Span::styled("CPU", Style::default().add_modifier(Modifier::BOLD).fg(Color::Yellow)),
    ]));
    let cores_used = r.cpu_pct / 100.0 * snapshot.host_ncpu as f64;
    let cpu_value = Paragraph::new(Line::from(vec![
        Span::styled(
            format!("{:.2}%", r.cpu_pct),
            Style::default().add_modifier(Modifier::BOLD).fg(severity_color(r.cpu_pct)),
        ),
        Span::raw("  "),
        Span::styled(
            if snapshot.host_ncpu > 0 {
                format!("{:.2} / {} cores", cores_used, snapshot.host_ncpu)
            } else {
                String::new()
            },
            Style::default().fg(Color::Cyan),
        ),
    ]));
    let cpu_gauge = Gauge::default()
        .gauge_style(Style::default().fg(severity_color(r.cpu_pct)))
        .ratio((r.cpu_pct / 100.0).clamp(0.0, 1.0))
        .label("");

    // MEM side
    let mem_label = Paragraph::new(Line::from(vec![Span::styled(
        "MEM",
        Style::default().add_modifier(Modifier::BOLD).fg(Color::Magenta),
    )]));
    let mem_value = Paragraph::new(Line::from(vec![
        Span::styled(
            format!("{:.2}%", r.mem_pct),
            Style::default().add_modifier(Modifier::BOLD).fg(severity_color(r.mem_pct)),
        ),
        Span::raw("  "),
        Span::styled(
            format!(
                "{} / {}",
                format_size(r.mem_usage, BINARY),
                format_size(r.mem_limit, BINARY)
            ),
            Style::default().fg(Color::Cyan),
        ),
    ]));
    let mem_gauge = Gauge::default()
        .gauge_style(Style::default().fg(severity_color(r.mem_pct)))
        .ratio((r.mem_pct / 100.0).clamp(0.0, 1.0))
        .label("");

    f.render_widget(cpu_label, cpu_rows[0]);
    f.render_widget(cpu_value, cpu_rows[1]);
    f.render_widget(cpu_gauge, cpu_rows[2]);
    f.render_widget(mem_label, mem_rows[0]);
    f.render_widget(mem_value, mem_rows[1]);
    f.render_widget(mem_gauge, mem_rows[2]);
}

fn draw_footer(f: &mut Frame, area: Rect, filter: &str, editing_filter: bool, view: &View) {
    if matches!(view, View::Detail(_)) {
        // Hints live inside the popup; leave the footer blank.
        return;
    }
    let line = if editing_filter {
        Line::from(vec![
            Span::styled(
                " filter ",
                Style::default()
                    .bg(Color::Yellow)
                    .fg(Color::Black)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" /"),
            Span::styled(filter.to_string(), Style::default().add_modifier(Modifier::BOLD)),
            Span::styled("█", Style::default().fg(Color::Yellow)),
            Span::raw("    "),
            Span::styled("Enter", Style::default().fg(Color::Cyan)),
            Span::raw(" apply  "),
            Span::styled("Esc", Style::default().fg(Color::Cyan)),
            Span::raw(" clear"),
        ])
    } else {
        Line::from(vec![
            Span::styled(" q/Esc ", Style::default().fg(Color::Cyan)),
            Span::raw("quit  "),
            Span::styled(" ↑/↓ ", Style::default().fg(Color::Cyan)),
            Span::raw("select  "),
            Span::styled(" s ", Style::default().fg(Color::Cyan)),
            Span::raw("cycle  "),
            Span::styled(" c/m/n ", Style::default().fg(Color::Cyan)),
            Span::raw("sort cpu/mem/name  "),
            Span::styled(" / ", Style::default().fg(Color::Cyan)),
            Span::raw("filter  "),
            Span::styled(" Enter ", Style::default().fg(Color::Cyan)),
            Span::raw("details"),
        ])
    };
    f.render_widget(Paragraph::new(line), area);
}

fn severity_color(pct: f64) -> Color {
    if pct >= 80.0 {
        Color::Red
    } else if pct >= 50.0 {
        Color::Yellow
    } else {
        Color::Green
    }
}

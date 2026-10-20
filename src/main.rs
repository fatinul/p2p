//! P2P Room: a tiny "anyone can host" chat, drawn as a node graph in the terminal.
//!
//! Topology (like an old listen-server): one player hosts, everyone else joins.
//! Every client talks to the host, and the host relays messages between clients.
//!
//! Wire protocol: one UTF-8 line per frame, fields separated by TAB.
//! Names and message text are sanitized so they can never contain TAB/newline.
//!
//!   client -> host   HELLO\t<name>
//!                    MSG\t<to_id>\t<text>        (to_id 0 = host, u32::MAX = everyone)
//!   host   -> client WELCOME\t<your_id>\t<host_name>
//!                    JOIN\t<id>\t<name>
//!                    LEAVE\t<id>
//!                    MSG\t<from_id>\t<text>
//!                    ERR\t<reason>

use crossterm::{
    cursor::Show,
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind, EnableMouseCapture, DisableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line as TextLine, Span},
    widgets::{
        canvas::{Canvas, Line},
        Block, Borders, Clear, Paragraph,
    },
    Frame, Terminal,
};
use std::{
    collections::BTreeMap,
    fs::{OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket},
    sync::{
        atomic::{AtomicU32, Ordering},
        mpsc::{self, Receiver, Sender},
        Arc, Mutex, MutexGuard,
    },
    thread,
    time::{Duration, Instant},
};

// ───────────────────────────── constants ─────────────────────────────

const NODE_W: u16 = 22;
const NODE_H: u16 = 4;
const MIN_TERM_W: u16 = 72;
const MIN_TERM_H: u16 = 20;

const MAX_PEERS: usize = 12;
const MAX_NAME_CHARS: usize = 16;
const MAX_TEXT_CHARS: usize = 400;
const MAX_LOG: usize = 100;
const MAX_LINE_BYTES: u64 = 2048;

const SERVER_ID: u32 = 0;
const BROADCAST_ID: u32 = u32::MAX;

const NET_TIMEOUT: Duration = Duration::from_secs(5);

const WORD_STAGGER: f32 = 0.15; // seconds between words leaving the node
const WORD_FLIGHT: f32 = 0.9; // seconds one word needs to cross the link
const MAX_FLYING_WORDS: usize = 6;

const HISTORY_FILE: &str = "p2p_chat_history.txt";

// ───────────────────────────── small helpers ─────────────────────────────

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Display width of one char (handles wide/zero-width chars).
fn cw(c: char) -> usize {
    Span::raw(c.to_string()).width()
}

fn text_width(s: &str) -> usize {
    s.chars().map(cw).sum()
}

/// Replace control chars (incl. TAB/newline) with spaces, trim, cap length.
fn sanitize(s: &str, max_chars: usize) -> String {
    let cleaned: String = s.chars().map(|c| if c.is_control() { ' ' } else { c }).collect();
    cleaned.trim().chars().take(max_chars).collect::<String>().trim_end().to_string()
}

fn display_name(raw: &str) -> String {
    let n = sanitize(raw, MAX_NAME_CHARS);
    if n.is_empty() {
        "Anonymous".to_string()
    } else {
        n
    }
}

fn truncate(s: &str, max_w: usize) -> String {
    if text_width(s) <= max_w {
        return s.to_string();
    }
    let mut out = String::new();
    let mut w = 0;
    for c in s.chars() {
        let c_w = cw(c);
        if w + c_w + 1 > max_w {
            break;
        }
        out.push(c);
        w += c_w;
    }
    out.push('~');
    out
}

/// Keep the *end* of `s` so that it fits in `max_w` columns (for the input box).
fn tail_fit(s: &str, max_w: usize) -> String {
    let mut w = 0;
    let mut out: Vec<char> = Vec::new();
    for c in s.chars().rev() {
        let c_w = cw(c);
        if w + c_w > max_w {
            break;
        }
        w += c_w;
        out.push(c);
    }
    out.into_iter().rev().collect()
}

fn wrap_text(s: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    let mut cur = String::new();
    let mut cur_w = 0usize;
    for word in s.split_whitespace() {
        let ww = text_width(word);
        if cur_w > 0 && cur_w + 1 + ww <= width {
            cur.push(' ');
            cur.push_str(word);
            cur_w += 1 + ww;
            continue;
        }
        if cur_w > 0 {
            lines.push(std::mem::take(&mut cur));
            cur_w = 0;
        }
        if ww <= width {
            cur.push_str(word);
            cur_w = ww;
        } else {
            for c in word.chars() {
                let w = cw(c);
                if cur_w + w > width && cur_w > 0 {
                    lines.push(std::mem::take(&mut cur));
                    cur_w = 0;
                }
                cur.push(c);
                cur_w += w;
            }
        }
    }
    if !cur.is_empty() || lines.is_empty() {
        lines.push(cur);
    }
    lines
}

fn lan_ip() -> IpAddr {
    UdpSocket::bind("0.0.0.0:0")
        .and_then(|s| {
            s.connect("192.0.2.1:9")?;
            s.local_addr()
        })
        .map(|a| a.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST))
}

// ───────────────────────────── persistence ─────────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    In,
    Out,
    Sys,
}

struct LogEntry {
    kind: Kind,
    text: String,
}

fn save_history_entry(peer_name: &str, kind: Kind, text: &str) {
    let kind_str = match kind {
        Kind::In => "IN",
        Kind::Out => "OUT",
        Kind::Sys => "SYS",
    };
    let clean_text = text.replace('\t', " ");
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(HISTORY_FILE) {
        let _ = writeln!(file, "{}\t{}\t{}", peer_name, kind_str, clean_text);
    }
}

fn load_history_for(peer_name: &str) -> Vec<LogEntry> {
    let mut logs = Vec::new();
    if let Ok(file) = std::fs::File::open(HISTORY_FILE) {
        let reader = BufReader::new(file);
        for line in reader.lines().flatten() {
            let mut parts = line.splitn(3, '\t');
            if let (Some(name), Some(k_str), Some(txt)) = (parts.next(), parts.next(), parts.next()) {
                if name == peer_name {
                    let kind = match k_str {
                        "IN" => Kind::In,
                        "OUT" => Kind::Out,
                        _ => Kind::Sys,
                    };
                    logs.push(LogEntry { kind, text: txt.to_string() });
                }
            }
        }
    }
    if logs.is_empty() {
        logs.push(LogEntry { kind: Kind::Sys, text: "Connected to room".to_string() });
    }
    logs
}

// ───────────────────────────── networking ─────────────────────────────

enum NetEvent {
    PeerJoined { id: u32, name: String },
    PeerLeft { id: u32 },
    Message { from: u32, text: String },
    Disconnected(String),
}

fn read_frame<R: BufRead>(reader: &mut R, buf: &mut String) -> io::Result<bool> {
    buf.clear();
    let n = reader.by_ref().take(MAX_LINE_BYTES).read_line(buf)?;
    if n == 0 {
        return Ok(false);
    }
    if !buf.ends_with('\n') {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "line too long or truncated"));
    }
    while buf.ends_with('\n') || buf.ends_with('\r') {
        buf.pop();
    }
    Ok(true)
}

fn spawn_writer(mut stream: TcpStream, rx: Receiver<String>) {
    thread::spawn(move || {
        let _ = stream.set_write_timeout(Some(NET_TIMEOUT));
        for line in rx {
            let mut data = line.into_bytes();
            data.push(b'\n');
            if stream.write_all(&data).is_err() {
                break;
            }
        }
        let _ = stream.shutdown(Shutdown::Both);
    });
}

struct ClientHandle {
    name: String,
    tx: Sender<String>,
}
type Hub = Arc<Mutex<BTreeMap<u32, ClientHandle>>>;

struct HostHandle {
    hub: Hub,
    local_addr: SocketAddr,
}

impl HostHandle {
    fn send(&self, to: u32, text: &str) -> bool {
        let wire = format!("MSG\t{SERVER_ID}\t{text}");
        let clients = lock(&self.hub);
        if to == BROADCAST_ID {
            clients.values().fold(false, |ok, c| c.tx.send(wire.clone()).is_ok() || ok)
        } else {
            clients.get(&to).map(|c| c.tx.send(wire).is_ok()).unwrap_or(false)
        }
    }
}

fn start_host(bind: &str, host_name: String, ui_tx: Sender<NetEvent>) -> io::Result<HostHandle> {
    let listener = TcpListener::bind(bind)?;
    let local_addr = listener.local_addr()?;
    let hub: Hub = Arc::new(Mutex::new(BTreeMap::new()));
    let next_id = Arc::new(AtomicU32::new(1));

    let accept_hub = hub.clone();
    thread::spawn(move || {
        for conn in listener.incoming() {
            match conn {
                Ok(stream) => {
                    let (hub, next_id) = (accept_hub.clone(), next_id.clone());
                    let (name, tx) = (host_name.clone(), ui_tx.clone());
                    thread::spawn(move || handle_client(stream, hub, next_id, name, tx));
                }
                Err(_) => thread::sleep(Duration::from_millis(100)),
            }
        }
    });

    Ok(HostHandle { hub, local_addr })
}

fn handle_client(
    stream: TcpStream,
    hub: Hub,
    next_id: Arc<AtomicU32>,
    host_name: String,
    ui_tx: Sender<NetEvent>,
) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(NET_TIMEOUT));
    let Ok(write_half) = stream.try_clone() else { return };
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    if !matches!(read_frame(&mut reader, &mut line), Ok(true)) {
        return;
    }
    let Some(raw_name) = line.strip_prefix("HELLO\t") else { return };
    let name = display_name(raw_name);

    let (tx, rx) = mpsc::channel::<String>();
    spawn_writer(write_half, rx);

    let id = {
        let mut clients = lock(&hub);
        if clients.len() >= MAX_PEERS {
            let _ = tx.send("ERR\tRoom is full".to_string());
            return;
        }
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        let _ = tx.send(format!("WELCOME\t{id}\t{host_name}"));
        for (eid, e) in clients.iter() {
            let _ = tx.send(format!("JOIN\t{eid}\t{}", e.name));
        }
        for e in clients.values() {
            let _ = e.tx.send(format!("JOIN\t{id}\t{name}"));
        }
        clients.insert(id, ClientHandle { name: name.clone(), tx: tx.clone() });
        id
    };
    let _ = ui_tx.send(NetEvent::PeerJoined { id, name });
    let _ = reader.get_ref().set_read_timeout(None);

    while matches!(read_frame(&mut reader, &mut line), Ok(true)) {
        let Some(rest) = line.strip_prefix("MSG\t") else { continue };
        let Some((to, text)) = rest.split_once('\t') else { continue };
        let Ok(to) = to.parse::<u32>() else { continue };
        let text = sanitize(text, MAX_TEXT_CHARS);
        if !text.is_empty() {
            route_message(&hub, id, to, text, &ui_tx);
        }
    }

    {
        let mut clients = lock(&hub);
        clients.remove(&id);
        for e in clients.values() {
            let _ = e.tx.send(format!("LEAVE\t{id}"));
        }
    }
    let _ = ui_tx.send(NetEvent::PeerLeft { id });
}

fn route_message(hub: &Hub, from: u32, to: u32, text: String, ui_tx: &Sender<NetEvent>) {
    let wire = format!("MSG\t{from}\t{text}");
    match to {
        SERVER_ID => {
            let _ = ui_tx.send(NetEvent::Message { from, text });
        }
        BROADCAST_ID => {
            for (cid, c) in lock(hub).iter() {
                if *cid != from {
                    let _ = c.tx.send(wire.clone());
                }
            }
            let _ = ui_tx.send(NetEvent::Message { from, text });
        }
        other => {
            if let Some(c) = lock(hub).get(&other) {
                let _ = c.tx.send(wire);
            }
        }
    }
}

struct ClientLink {
    tx: Sender<String>,
    my_id: u32,
    server_name: String,
}

fn connect_client(addr: &str, my_name: &str, ui_tx: Sender<NetEvent>) -> Result<ClientLink, String> {
    let sock_addr = addr
        .to_socket_addrs()
        .map_err(|e| format!("Bad address '{addr}': {e}"))?
        .next()
        .ok_or_else(|| format!("Could not resolve '{addr}'"))?;
    let stream = TcpStream::connect_timeout(&sock_addr, NET_TIMEOUT)
        .map_err(|e| format!("Cannot connect to {addr}: {e}"))?;
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(NET_TIMEOUT));
    let write_half = stream.try_clone().map_err(|e| e.to_string())?;

    let (tx, rx) = mpsc::channel::<String>();
    spawn_writer(write_half, rx);
    let _ = tx.send(format!("HELLO\t{my_name}"));

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    match read_frame(&mut reader, &mut line) {
        Ok(true) => {}
        Ok(false) => return Err("Server closed the connection".to_string()),
        Err(e) => return Err(format!("No valid reply from server: {e}")),
    }
    let bad = || "That does not look like a P2P Room server".to_string();
    let (my_id, server_name) = if let Some(rest) = line.strip_prefix("WELCOME\t") {
        let (id, name) = rest.split_once('\t').ok_or_else(bad)?;
        (id.parse::<u32>().map_err(|_| bad())?, display_name(name))
    } else if let Some(reason) = line.strip_prefix("ERR\t") {
        return Err(sanitize(reason, 80));
    } else {
        return Err(bad());
    };

    let _ = reader.get_ref().set_read_timeout(None);
    thread::spawn(move || client_reader(reader, ui_tx));
    Ok(ClientLink { tx, my_id, server_name })
}

fn client_reader(mut reader: BufReader<TcpStream>, ui_tx: Sender<NetEvent>) {
    let mut line = String::new();
    let mut reason = "The server closed the connection".to_string();
    loop {
        match read_frame(&mut reader, &mut line) {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => {
                reason = format!("Connection lost: {e}");
                break;
            }
        }
        let mut parts = line.splitn(3, '\t');
        let event = match (parts.next(), parts.next(), parts.next()) {
            (Some("JOIN"), Some(id), Some(name)) => id
                .parse()
                .ok()
                .map(|id| NetEvent::PeerJoined { id, name: display_name(name) }),
            (Some("LEAVE"), Some(id), _) => id.parse().ok().map(|id| NetEvent::PeerLeft { id }),
            (Some("MSG"), Some(from), Some(text)) => from
                .parse()
                .ok()
                .map(|from| NetEvent::Message { from, text: sanitize(text, MAX_TEXT_CHARS) }),
            (Some("ERR"), Some(r), _) => {
                reason = sanitize(r, 80);
                break;
            }
            _ => None,
        };
        if let Some(ev) = event {
            if ui_tx.send(ev).is_err() {
                return;
            }
        }
    }
    let _ = ui_tx.send(NetEvent::Disconnected(reason));
}

// ───────────────────────────── app state ─────────────────────────────

#[derive(PartialEq, Clone, Copy)]
enum Screen {
    Wizard,
    Chat,
}

#[derive(PartialEq, Clone, Copy)]
enum Mode {
    Host,
    Join,
}

struct Wizard {
    mode: Mode,
    name: String,
    addr: String,
    field: usize,
    error: Option<String>,
}

struct Peer {
    id: u32,
    name: String,
    manual: Option<(f32, f32)>,
    log: Vec<LogEntry>,
    total: usize,
    unread: usize,
}

impl Peer {
    fn new(id: u32, name: String) -> Self {
        let log = load_history_for(&name);
        let total = log.iter().filter(|e| e.kind != Kind::Sys).count();
        Self {
            id,
            name,
            manual: None,
            log,
            total,
            unread: 0,
        }
    }
}

struct Flight {
    peer_id: u32,
    full: String,
    words: Vec<String>,
    t: f32,
    incoming: bool,
}

impl Flight {
    fn new(peer_id: u32, text: String, incoming: bool) -> Self {
        let all: Vec<&str> = text.split_whitespace().collect();
        let mut words: Vec<String> = all.iter().take(MAX_FLYING_WORDS).map(|w| truncate(w, 14)).collect();
        if all.len() > MAX_FLYING_WORDS || words.is_empty() {
            words.push("...".to_string());
        }
        Self { peer_id, full: text, words, t: 0.0, incoming }
    }
    fn duration(&self) -> f32 {
        self.words.len().saturating_sub(1) as f32 * WORD_STAGGER + WORD_FLIGHT
    }
    fn started_words(&self) -> usize {
        (0..self.words.len()).filter(|&i| self.t >= i as f32 * WORD_STAGGER).count()
    }
}

enum Link {
    None,
    Host(HostHandle),
    Client(ClientLink),
}

struct App {
    screen: Screen,
    wizard: Wizard,
    should_quit: bool,
    name: String,
    is_host: bool,
    peers: Vec<Peer>,
    selected: usize,
    input: String,
    flights: Vec<Flight>,
    ui_tx: Sender<NetEvent>,
    ui_rx: Receiver<NetEvent>,
    link: Link,
    my_id: u32,
    room_info: String,
    lan: IpAddr,
    status: Option<(String, Instant)>,
    free_range: (f32, f32),
    // Hit-testing state for mouse clicks
    last_peer_rects: Vec<(u32, Rect)>,
}

fn outline_point(s: f32, fx: f32, fy: f32) -> (f32, f32) {
    let mut s = s.rem_euclid(2.0 * (fx + fy));
    if s < fy / 2.0 {
        return (1.0, 0.5 + s / fy);
    }
    s -= fy / 2.0;
    if s < fx {
        return (1.0 - s / fx, 1.0);
    }
    s -= fx;
    if s < fy {
        return (0.0, 1.0 - s / fy);
    }
    s -= fy;
    if s < fx {
        return (s / fx, 0.0);
    }
    s -= fx;
    (1.0, s / fy)
}

fn auto_ratio(i: usize, n: usize, fx: f32, fy: f32) -> (f32, f32) {
    let (fx, fy) = (fx.max(1.0), fy.max(1.0));
    let start = if n > 1 && n % 2 == 1 { fy / 2.0 + fx + fy + fx / 2.0 } else { 0.0 };
    outline_point(start + 2.0 * (fx + fy) * i as f32 / n.max(1) as f32, fx, fy)
}

impl App {
    fn new() -> Self {
        let (ui_tx, ui_rx) = mpsc::channel();
        Self {
            screen: Screen::Wizard,
            wizard: Wizard {
                mode: Mode::Host,
                name: "Player-One".to_string(),
                addr: "8001".to_string(),
                field: 0,
                error: None,
            },
            should_quit: false,
            name: String::new(),
            is_host: true,
            peers: Vec::new(),
            selected: 0,
            input: String::new(),
            flights: Vec::new(),
            ui_tx,
            ui_rx,
            link: Link::None,
            my_id: SERVER_ID,
            room_info: String::new(),
            lan: lan_ip(),
            status: None,
            free_range: (1.0, 1.0),
            last_peer_rects: Vec::new(),
        }
    }

    fn set_status(&mut self, msg: impl Into<String>) {
        self.status = Some((msg.into(), Instant::now()));
    }

    fn clamp_selection(&mut self) {
        if self.selected >= self.peers.len() {
            self.selected = self.peers.len().saturating_sub(1);
        }
    }

    fn select(&mut self, idx: usize) {
        if self.peers.is_empty() {
            return;
        }
        self.selected = idx % self.peers.len();
        self.peers[self.selected].unread = 0;
    }

    fn start_session(&mut self) -> Result<(), String> {
        let name = display_name(&self.wizard.name);
        let addr = self.wizard.addr.trim().to_string();
        let is_host = self.wizard.mode == Mode::Host;

        if is_host {
            let bind = if addr.contains(':') {
                addr
            } else {
                let port: u16 = addr.parse().map_err(|_| "Port must be a number (or use ip:port)".to_string())?;
                format!("0.0.0.0:{port}")
            };
            let host = start_host(&bind, name.clone(), self.ui_tx.clone())
                .map_err(|e| format!("Cannot host on {bind}: {e}"))?;
            self.room_info = format!("friends join: {}:{}", self.lan, host.local_addr.port());
            self.link = Link::Host(host);
            self.my_id = SERVER_ID;
        } else {
            let target = if addr.contains(':') { addr } else { format!("127.0.0.1:{addr}") };
            let link = connect_client(&target, &name, self.ui_tx.clone())?;
            self.room_info = format!("server: {target}");
            self.my_id = link.my_id;
            self.peers.push(Peer::new(SERVER_ID, link.server_name.clone()));
            self.link = Link::Client(link);
        }

        self.name = name;
        self.is_host = is_host;
        self.selected = 0;
        self.wizard.error = None;
        self.screen = Screen::Chat;
        Ok(())
    }

    fn reset_to_wizard(&mut self, msg: String) {
        self.screen = Screen::Wizard;
        self.peers.clear();
        self.flights.clear();
        self.selected = 0;
        self.input.clear();
        self.link = Link::None;
        self.status = None;
        self.wizard.error = Some(msg);
    }

    fn handle_key(&mut self, key: KeyEvent) {
        match self.screen {
            Screen::Wizard => self.handle_wizard_key(key),
            Screen::Chat => self.handle_chat_key(key),
        }
    }

    fn handle_mouse(&mut self, mouse: MouseEvent) {
        if self.screen != Screen::Chat {
            return;
        }
        if let MouseEventKind::Down(MouseButton::Left) = mouse.kind {
            let (mx, my) = (mouse.column, mouse.row);
            for (i, (_id, r)) in self.last_peer_rects.iter().enumerate() {
                if mx >= r.x && mx < r.x + r.width && my >= r.y && my < r.y + r.height {
                    self.select(i);
                    break;
                }
            }
        }
    }

    fn handle_wizard_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let alt = key.modifiers.contains(KeyModifiers::ALT);
        let w = &mut self.wizard;
        match key.code {
            KeyCode::Char('c') if ctrl => self.should_quit = true,
            KeyCode::Esc => self.should_quit = true,
            KeyCode::Tab | KeyCode::Down => w.field = (w.field + 1) % 3,
            KeyCode::BackTab | KeyCode::Up => w.field = (w.field + 2) % 3,
            KeyCode::Left | KeyCode::Right if w.field == 0 => {
                w.mode = if w.mode == Mode::Host { Mode::Join } else { Mode::Host };
                w.addr = if w.mode == Mode::Host { "8001" } else { "127.0.0.1:8001" }.to_string();
            }
            KeyCode::Enter => {
                if let Err(e) = self.start_session() {
                    self.wizard.error = Some(e);
                }
            }
            KeyCode::Char(c) if (!ctrl || alt) && !c.is_control() => match w.field {
                1 if w.name.chars().count() < MAX_NAME_CHARS => w.name.push(c),
                2 if w.addr.chars().count() < 64 => w.addr.push(c),
                _ => {}
            },
            KeyCode::Backspace => match w.field {
                1 => { w.name.pop(); }
                2 => { w.addr.pop(); }
                _ => {}
            },
            _ => {}
        }
    }

    fn handle_chat_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let alt = key.modifiers.contains(KeyModifiers::ALT);

        if ctrl && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }
        if ctrl && key.code == KeyCode::Char('r') {
            for p in &mut self.peers {
                p.manual = None;
            }
            self.set_status("Layout reset");
            return;
        }
        if ctrl || alt {
            match key.code {
                KeyCode::Left => self.move_selected(-2, 0),
                KeyCode::Right => self.move_selected(2, 0),
                KeyCode::Up => self.move_selected(0, -1),
                KeyCode::Down => self.move_selected(0, 1),
                KeyCode::Char(c) if alt && !ctrl && !c.is_control() => self.push_input(c),
                _ => {}
            }
            return;
        }

        match key.code {
            KeyCode::Tab | KeyCode::Right => self.select(self.selected + 1),
            KeyCode::BackTab | KeyCode::Left => self.select(self.selected + self.peers.len().saturating_sub(1)),
            KeyCode::Enter => self.submit(),
            KeyCode::Char(c) if !c.is_control() => self.push_input(c),
            KeyCode::Backspace => {
                self.input.pop();
            }
            KeyCode::Esc => self.should_quit = true,
            _ => {}
        }
    }

    fn push_input(&mut self, c: char) {
        if self.input.chars().count() < MAX_TEXT_CHARS + 5 {
            self.input.push(c);
        }
    }

    fn move_selected(&mut self, dcol: i32, drow: i32) {
        let n = self.peers.len();
        if n == 0 {
            return;
        }
        let (fx, fy) = self.free_range;
        let sel = self.selected.min(n - 1);
        let (u, v) = self.peers[sel].manual.unwrap_or_else(|| auto_ratio(sel, n, fx, fy));
        let nu = (u + dcol as f32 / fx.max(1.0)).clamp(0.0, 1.0);
        let nv = (v + drow as f32 / fy.max(1.0)).clamp(0.0, 1.0);
        self.peers[sel].manual = Some((nu, nv));
    }

    fn submit(&mut self) {
        let raw = self.input.trim().to_string();
        if raw.is_empty() {
            return;
        }
        if raw == "/all" {
            self.set_status("Usage: /all <message>   (sends to everyone)");
            return;
        }
        let (broadcast, body) = match raw.strip_prefix("/all ") {
            Some(rest) => (true, rest.to_string()),
            None => (false, raw),
        };
        let text = sanitize(&body, MAX_TEXT_CHARS);
        if text.is_empty() {
            self.input.clear();
            return;
        }
        if self.peers.is_empty() {
            self.set_status("Nobody here yet - wait for someone to join");
            return;
        }

        let (to, targets): (u32, Vec<u32>) = if broadcast {
            (BROADCAST_ID, self.peers.iter().map(|p| p.id).collect())
        } else {
            let id = self.peers[self.selected.min(self.peers.len() - 1)].id;
            (id, vec![id])
        };
        let ok = match &self.link {
            Link::Host(h) => h.send(to, &text),
            Link::Client(c) => c.tx.send(format!("MSG\t{to}\t{text}")).is_ok(),
            Link::None => false,
        };
        if !ok {
            self.set_status("Message could not be delivered");
            return;
        }
        for id in targets {
            self.flights.push(Flight::new(id, text.clone(), false));
        }
        self.input.clear();
    }

    fn poll_network(&mut self) {
        while let Ok(ev) = self.ui_rx.try_recv() {
            match ev {
                NetEvent::PeerJoined { id, name } => {
                    if (id == self.my_id && !self.is_host) || self.peers.iter().any(|p| p.id == id) {
                        continue;
                    }
                    self.set_status(format!("{name} joined"));
                    self.peers.push(Peer::new(id, name));
                }
                NetEvent::PeerLeft { id } => {
                    if let Some(idx) = self.peers.iter().position(|p| p.id == id) {
                        let gone = self.peers.remove(idx);
                        self.flights.retain(|f| f.peer_id != id);
                        if idx < self.selected {
                            self.selected -= 1;
                        }
                        self.clamp_selection();
                        self.set_status(format!("{} left", gone.name));
                    }
                }
                NetEvent::Message { from, text } => {
                    if self.peers.iter().any(|p| p.id == from) {
                        self.flights.push(Flight::new(from, text, true));
                    }
                }
                NetEvent::Disconnected(reason) => {
                    self.reset_to_wizard(reason);
                    return;
                }
            }
        }
    }

    fn tick(&mut self, dt: f32) {
        for f in &mut self.flights {
            f.t += dt;
        }
        let mut blocked: Vec<u32> = Vec::new();
        let mut i = 0;
        while i < self.flights.len() {
            let pid = self.flights[i].peer_id;
            if self.flights[i].t >= self.flights[i].duration() && !blocked.contains(&pid) {
                let f = self.flights.remove(i);
                self.commit(f);
            } else {
                if !blocked.contains(&pid) {
                    blocked.push(pid);
                }
                i += 1;
            }
        }
    }

    fn commit(&mut self, f: Flight) {
        let selected_id = self.peers.get(self.selected).map(|p| p.id);
        if let Some(p) = self.peers.iter_mut().find(|p| p.id == f.peer_id) {
            p.total += 1;
            if f.incoming && selected_id != Some(p.id) {
                p.unread += 1;
            }
            let kind = if f.incoming { Kind::In } else { Kind::Out };
            save_history_entry(&p.name, kind, &f.full);
            p.log.push(LogEntry { kind, text: f.full });
            if p.log.len() > MAX_LOG {
                p.log.remove(0);
            }
        }
    }
}

// ───────────────────────────── geometry ─────────────────────────────

type Pt = (f64, f64);

struct Geometry {
    local: Rect,
    peers: Vec<Rect>,
    free: (f32, f32),
}

fn geometry(inner: Rect, peers: &[Peer]) -> Geometry {
    let fx = inner.width.saturating_sub(NODE_W) as f32;
    let fy = inner.height.saturating_sub(NODE_H) as f32;
    let local = Rect::new(
        inner.x + (inner.width.saturating_sub(NODE_W)) / 2,
        inner.y + (inner.height.saturating_sub(NODE_H)) / 2,
        NODE_W,
        NODE_H,
    );
    let n = peers.len();
    let rects = peers
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let (u, v) = p.manual.unwrap_or_else(|| auto_ratio(i, n, fx, fy));
            Rect::new(
                inner.x + (u.clamp(0.0, 1.0) * fx).round() as u16,
                inner.y + (v.clamp(0.0, 1.0) * fy).round() as u16,
                NODE_W,
                NODE_H,
            )
        })
        .collect();
    Geometry { local, peers: rects, free: (fx, fy) }
}

fn rect_center(r: Rect, inner: Rect) -> Pt {
    (
        (r.x - inner.x) as f64 + r.width as f64 / 2.0,
        (r.y - inner.y) as f64 + r.height as f64 / 2.0,
    )
}

fn edge_point(center: Pt, half: Pt, target: Pt) -> Pt {
    let (dx, dy) = (target.0 - center.0, target.1 - center.1);
    if dx.abs() < 1e-9 && dy.abs() < 1e-9 {
        return center;
    }
    let sx = if dx.abs() > 1e-9 { half.0 / dx.abs() } else { f64::INFINITY };
    let sy = if dy.abs() > 1e-9 { half.1 / dy.abs() } else { f64::INFINITY };
    let s = sx.min(sy);
    (center.0 + dx * s, center.1 + dy * s)
}

fn link_segment(a: Rect, b: Rect, inner: Rect) -> Option<(Pt, Pt)> {
    if a.intersects(b) {
        return None;
    }
    let (ca, cb) = (rect_center(a, inner), rect_center(b, inner));
    let half = |r: Rect| ((r.width as f64 - 1.0) / 2.0, (r.height as f64 - 1.0) / 2.0);
    Some((edge_point(ca, half(a), cb), edge_point(cb, half(b), ca)))
}

struct CanvasMap {
    w: f64,
    h: f64,
}

impl CanvasMap {
    fn new(inner: Rect) -> Self {
        Self { w: inner.width as f64, h: inner.height as f64 }
    }
    fn x_bounds(&self) -> [f64; 2] {
        [0.0, self.w - 0.5]
    }
    fn y_bounds(&self) -> [f64; 2] {
        [0.0, self.h - 0.25]
    }
    fn pt(&self, p: Pt) -> Pt {
        let top = self.h - 0.25;
        (p.0.clamp(0.0, self.w - 0.5), top - p.1.clamp(0.0, top))
    }
}

fn place_bubble(node_idx: usize, size: (u16, u16), inner: Rect, local: Rect, nodes: &[Rect]) -> Rect {
    #[derive(Clone, Copy, PartialEq)]
    enum Side { Below, Above, Right, Left }
    let node = nodes[node_idx];
    let (min_w, min_h) = (22.min(size.0), 4.min(size.1));
    let (ix0, iy0) = (inner.x as i32, inner.y as i32);
    let (ix1, iy1) = (inner.right() as i32, inner.bottom() as i32);
    let (nx, ny, nw, nh) = (node.x as i32, node.y as i32, node.width as i32, node.height as i32);

    let down = node.y + node.height / 2 >= local.y + local.height / 2;
    let rightwards = node.x + node.width / 2 >= local.x + local.width / 2;
    let v = if down { [Side::Below, Side::Above] } else { [Side::Above, Side::Below] };
    let h = if rightwards { [Side::Right, Side::Left] } else { [Side::Left, Side::Right] };
    let sides = [v[0], h[0], v[1], h[1]];

    let full = size.0 as u32 * size.1 as u32;
    let overlap = |a: Rect, b: Rect| a.intersection(b).area();
    let mut best: Option<(u32, Rect)> = None;

    for side in sides {
        let vertical = matches!(side, Side::Below | Side::Above);
        let steps = if vertical { size.1 - min_h } else { size.0 - min_w };
        for shrink in (0..=steps).step_by(1) {
            let (w, hgt) = if vertical { (size.0, size.1 - shrink) } else { (size.0 - shrink, size.1) };
            let (bw, bh) = (w as i32, hgt as i32);
            let cx = (nx + nw / 2 - bw / 2).clamp(ix0, (ix1 - bw).max(ix0));
            let cy = (ny + nh / 2 - bh / 2).clamp(iy0, (iy1 - bh).max(iy0));
            let (x, y) = match side {
                Side::Below => (cx, ny + nh),
                Side::Above => (cx, ny - bh),
                Side::Right => (nx + nw, cy),
                Side::Left => (nx - bw, cy),
            };
            if x < ix0 || y < iy0 || x + bw > ix1 || y + bh > iy1 {
                continue;
            }
            let r = Rect::new(x as u16, y as u16, w, hgt);
            let mut score = overlap(r, node) * 10 + overlap(r, local) * 4 + (full - r.area()) / 6;
            for (i, o) in nodes.iter().enumerate() {
                if i != node_idx {
                    score += overlap(r, *o);
                }
            }
            if best.map_or(true, |(s, _)| score < s) {
                best = Some((score, r));
            }
        }
    }
    best.map(|(_, r)| r).unwrap_or_else(|| {
        let x = (nx).clamp(ix0, (ix1 - size.0 as i32).max(ix0));
        let y = (ny + nh).clamp(iy0, (iy1 - size.1 as i32).max(iy0));
        Rect::new(x as u16, y as u16, size.0, size.1)
    })
}

// ───────────────────────────── drawing ─────────────────────────────

fn split_chat(area: Rect) -> (Rect, Rect, Rect, Rect) {
    let chunks = Layout::vertical([Constraint::Min(10), Constraint::Length(3), Constraint::Length(1)]).split(area);
    let inner = Block::default().borders(Borders::ALL).inner(chunks[0]);
    (chunks[0], inner, chunks[1], chunks[2])
}

fn ui(f: &mut Frame, app: &mut App) {
    match app.screen {
        Screen::Wizard => draw_wizard(f, app),
        Screen::Chat => draw_chat(f, app),
    }
}

fn draw_wizard(f: &mut Frame, app: &App) {
    let area = f.area();
    let w = &app.wizard;
    let layout = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Length(3),
        Constraint::Min(2),
    ])
    .margin(3)
    .split(area);

    f.render_widget(
        Block::default()
            .title(" P2P Room Setup ")
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::Gray)),
        area,
    );

    let field = |f: &mut Frame, rect: Rect, text: String, idx: usize| {
        let color = if w.field == idx { Color::Yellow } else { Color::DarkGray };
        f.render_widget(
            Paragraph::new(text).block(Block::default().borders(Borders::ALL).style(Style::default().fg(color))),
            rect,
        );
    };

    let mode_str = if w.mode == Mode::Host { "[ Host Server ]" } else { "[ Join Server ]" };
    field(f, layout[0], format!(" Mode: {mode_str} (Left/Right to toggle)"), 0);
    field(f, layout[1], format!(" Player Name: {}_", w.name), 1);
    let label = if w.mode == Mode::Host { "Listen Port" } else { "Server Address (IP:Port)" };
    field(f, layout[2], format!(" {label}: {}_", w.addr), 2);

    let mut lines: Vec<TextLine> = Vec::new();
    if let Some(err) = &w.error {
        lines.push(TextLine::from(Span::styled(format!(" ! {err}"), Style::default().fg(Color::Red))));
    } else if w.mode == Mode::Host {
        lines.push(TextLine::from(format!(" Friends on your network can join with {}:<port>", app.lan)));
    } else {
        lines.push(TextLine::from(""));
    }
    lines.push(TextLine::from(" [Tab/Up/Down] Navigate | [Left/Right] Toggle | [Enter] Start | [Esc] Quit"));
    f.render_widget(Paragraph::new(lines), layout[3]);
}

fn draw_chat(f: &mut Frame, app: &mut App) {
    let area = f.area();
    if area.width < MIN_TERM_W || area.height < MIN_TERM_H {
        f.render_widget(
            Paragraph::new(format!(
                "Terminal too small ({}x{}).\nNeed at least {}x{}. Resize the window, or press Esc to quit.",
                area.width, area.height, MIN_TERM_W, MIN_TERM_H
            )),
            area,
        );
        return;
    }

    let (canvas_area, inner, input_area, status_area) = split_chat(area);
    let geo = geometry(inner, &app.peers);
    app.free_range = geo.free;
    app.clamp_selection();

    // Cache rects for mouse hit-testing
    app.last_peer_rects = app.peers.iter().zip(&geo.peers).map(|(p, r)| (p.id, *r)).collect();

    let role = if app.is_host { "HOST" } else { "CLIENT" };
    f.render_widget(
        Block::default()
            .title(format!(
                " Room [{role}] {} | {} | Peers: {} ",
                app.name,
                app.room_info,
                app.peers.len()
            ))
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::Gray)),
        canvas_area,
    );

    let segs: Vec<Option<(Pt, Pt)>> = geo.peers.iter().map(|r| link_segment(geo.local, *r, inner)).collect();
    let map = CanvasMap::new(inner);
    let links: Vec<(Pt, Pt, bool)> = segs
        .iter()
        .enumerate()
        .filter_map(|(i, s)| s.map(|(a, b)| (map.pt(a), map.pt(b), i == app.selected)))
        .collect();

    let canvas = Canvas::default()
        .x_bounds(map.x_bounds())
        .y_bounds(map.y_bounds())
        .paint(move |ctx| {
            for selected in [false, true] {
                for (a, b, sel) in links.iter().filter(|l| l.2 == selected) {
                    let color = if *sel { Color::Yellow } else { Color::DarkGray };
                    ctx.draw(&Line { x1: a.0, y1: a.1, x2: b.0, y2: b.1, color });
                }
            }
        });
    f.render_widget(canvas, inner);

    for fl in &app.flights {
        let Some(idx) = app.peers.iter().position(|p| p.id == fl.peer_id) else { continue };
        let Some((a, b)) = segs[idx] else { continue };
        let (from, to) = if fl.incoming { (b, a) } else { (a, b) };
        let color = if fl.incoming { Color::Gray } else { Color::Yellow };
        for (i, word) in fl.words.iter().enumerate() {
            let s = ((fl.t - i as f32 * WORD_STAGGER) / WORD_FLIGHT) as f64;
            if !(0.0..1.0).contains(&s) {
                continue;
            }
            let label = format!("[{word}]");
            let lw = (text_width(&label) as u16).min(inner.width);
            let col = from.0 + (to.0 - from.0) * s;
            let row = from.1 + (to.1 - from.1) * s;
            let x = (inner.x as f64 + col - lw as f64 / 2.0).round().max(inner.x as f64) as u16;
            let x = x.min(inner.right().saturating_sub(lw));
            let y = ((inner.y as f64 + row).floor() as u16).clamp(inner.y, inner.bottom().saturating_sub(1));
            f.render_widget(Paragraph::new(Span::styled(label, Style::default().fg(color))), Rect::new(x, y, lw, 1));
        }
    }

    let draw_node = |f: &mut Frame, rect: Rect, title: &str, body: Vec<TextLine>, color: Color| {
        f.render_widget(Clear, rect);
        let block = Block::default()
            .title(format!(" {} ", truncate(title, (rect.width as usize).saturating_sub(4))))
            .borders(Borders::ALL)
            .style(Style::default().fg(color));
        f.render_widget(Paragraph::new(body).block(block), rect);
    };

    draw_node(
        f,
        geo.local,
        &format!("{} (You)", app.name),
        vec![TextLine::from(format!("Role: {role}")), TextLine::from("Status: Online")],
        Color::Gray,
    );
    for (i, peer) in app.peers.iter().enumerate() {
        let color = if i == app.selected { Color::Yellow } else { Color::DarkGray };
        let msgs = if peer.unread > 0 {
            format!("Msgs: {} (+{})", peer.total, peer.unread)
        } else {
            format!("Msgs: {}", peer.total)
        };
        let role = if peer.id == SERVER_ID { "Role: Host" } else { "Role: Player" };
        draw_node(f, geo.peers[i], &peer.name, vec![TextLine::from(role), TextLine::from(msgs)], color);
    }

    if let Some(peer) = app.peers.get(app.selected) {
        let size = (36u16.min(inner.width), 7u16.min(inner.height));
        let rect = place_bubble(app.selected, size, inner, geo.local, &geo.peers);
        f.render_widget(Clear, rect);
        let block = Block::default()
            .title(format!(" Log: {} ", truncate(&peer.name, 20)))
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::Yellow));
        let (text_w, text_h) = (rect.width.saturating_sub(2) as usize, rect.height.saturating_sub(2) as usize);

        let mut rows: Vec<(Color, String)> = Vec::new();
        let mut push = |kind: Kind, text: String| {
            let color = match kind {
                Kind::In => Color::Gray,
                Kind::Out => Color::Yellow,
                Kind::Sys => Color::DarkGray,
            };
            for l in wrap_text(&text, text_w) {
                rows.push((color, l));
            }
        };
        for e in &peer.log {
            let text = match e.kind {
                Kind::In => format!("In: {}", e.text),
                Kind::Out => format!("Out: {}", e.text),
                Kind::Sys => format!("* {}", e.text),
            };
            push(e.kind, text);
        }
        for fl in app.flights.iter().filter(|fl| fl.peer_id == peer.id) {
            let n = fl.started_words();
            if n > 0 {
                let tag = if fl.incoming { "In" } else { "Out" };
                push(if fl.incoming { Kind::In } else { Kind::Out }, format!("{tag}: {} ...", fl.words[..n].join(" ")));
            }
        }
        let skip = rows.len().saturating_sub(text_h);
        let lines: Vec<TextLine> = rows
            .into_iter()
            .skip(skip)
            .map(|(c, t)| TextLine::from(Span::styled(t, Style::default().fg(c))))
            .collect();
        f.render_widget(Paragraph::new(lines).block(block), rect);
    }

    let broadcast = app.input.starts_with("/all ");
    let target = if broadcast {
        "Everyone".to_string()
    } else if let Some(p) = app.peers.get(app.selected) {
        truncate(&p.name, 16)
    } else if app.is_host {
        "waiting for players...".to_string()
    } else {
        "nobody".to_string()
    };
    let prefix = format!("[To: {target}] > ");
    let avail = (input_area.width as usize).saturating_sub(2 + text_width(&prefix) + 1);
    f.render_widget(
        Paragraph::new(format!("{prefix}{}_", tail_fit(&app.input, avail))).block(
            Block::default()
                .title(" Message Input ")
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::Yellow)),
        ),
        input_area,
    );

    let status_line = match &app.status {
        Some((msg, at)) if at.elapsed() < Duration::from_secs(5) => {
            Span::styled(format!(" {msg}"), Style::default().fg(Color::Yellow))
        }
        _ => Span::styled(
            " Tab/Click: select | Alt+Arrows: move node | /all <msg>: everyone | Esc: quit",
            Style::default().fg(Color::DarkGray),
        ),
    };
    f.render_widget(Paragraph::new(status_line), status_area);
}

// ───────────────────────────── terminal plumbing ─────────────────────────────

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture, Show);
    }
}

fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if thread::current().name() == Some("main") {
            let _ = disable_raw_mode();
            let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture, Show);
        }
        default_hook(info);
    }));
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    install_panic_hook();
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let _guard = TerminalGuard;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;
    terminal.clear()?;

    let mut app = App::new();
    let mut last = Instant::now();

    while !app.should_quit {
        terminal.draw(|f| ui(f, &mut app))?;

        if event::poll(Duration::from_millis(33))? {
            loop {
                match event::read()? {
                    Event::Key(key) => {
                        if key.kind != KeyEventKind::Release {
                            app.handle_key(key);
                        }
                    }
                    Event::Mouse(mouse) => {
                        app.handle_mouse(mouse);
                    }
                    _ => {}
                }
                if !event::poll(Duration::ZERO)? {
                    break;
                }
            }
        }

        let now = Instant::now();
        let dt = (now - last).as_secs_f32().min(0.25);
        last = now;
        if app.screen == Screen::Chat {
            app.poll_network();
            app.tick(dt);
        }
    }
    Ok(())
}
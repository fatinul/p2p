use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    widgets::{Block, Borders, Clear, Paragraph},
    Terminal,
};
use std::{io, time::Duration};

struct Peer {
    name: String,
    x_ratio: f32, // Horizontal position as a percentage (0.0 to 1.0)
    y_ratio: f32, // Vertical position as a percentage (0.0 to 1.0)
    messages: Vec<String>,
}

struct App {
    should_quit: bool,
    peers: Vec<Peer>,
    selected_peer: usize,
    input_buffer: String,
}

impl App {
    fn new() -> Self {
        Self {
            should_quit: false,
            peers: vec![
                Peer {
                    name: "Node-Alpha (You)".to_string(),
                    x_ratio: 0.1,
                    y_ratio: 0.25,
                    messages: vec!["Local node initialized.".to_string()],
                },
                Peer {
                    name: "Node-Beta (Peer 1)".to_string(),
                    x_ratio: 0.4,
                    y_ratio: 0.55,
                    messages: vec!["Connected to network.".to_string()],
                },
                Peer {
                    name: "Node-Gamma (Peer 2)".to_string(),
                    x_ratio: 0.7,
                    y_ratio: 0.30,
                    messages: vec!["Syncing block headers...".to_string()],
                },
            ],
            selected_peer: 1,
            input_buffer: String::new(),
        }
    }

    fn handle_key(&mut self, key: KeyCode) {
        match key {
            KeyCode::Tab => {
                self.selected_peer += 1;
                if self.selected_peer >= self.peers.len() {
                    self.selected_peer = 1;
                }
            }
            KeyCode::Enter => {
                if !self.input_buffer.trim().is_empty() {
                    let msg = format!("Out: {}", self.input_buffer);
                    self.peers[self.selected_peer].messages.push(msg);
                    self.input_buffer.clear();
                }
            }
            KeyCode::Char(c) => {
                self.input_buffer.push(c);
            }
            KeyCode::Backspace => {
                self.input_buffer.pop();
            }
            KeyCode::Esc => {
                self.should_quit = true;
            }
            _ => {}
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new();

    while !app.should_quit {
        terminal.draw(|f| {
            let size = f.size();

            // Split screen: Top for responsive graph canvas, Bottom for fixed chat input
            let vertical_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(10),
                    Constraint::Length(3),
                ])
                .split(size);

            let canvas_area = vertical_chunks[0];
            let input_area = vertical_chunks[1];

            // 1. Render Main Graph Canvas Border
            let active_peer_name = &app.peers[app.selected_peer].name;
            let canvas_title = format!(
                " P2P Network Graph [Target: {} | Tab: Switch Peer | Enter: Send | Esc: Quit] ",
                active_peer_name
            );
            let canvas_block = Block::default()
                .title(canvas_title)
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::Cyan));
            
            f.render_widget(canvas_block, canvas_area);

            // 2. Render Responsive Nodes on the Canvas
            for (i, peer) in app.peers.iter().enumerate() {
                let is_target = i == app.selected_peer;
                let is_local = i == 0;
                
                let border_color = if is_local {
                    Color::Cyan
                } else if is_target {
                    Color::Yellow
                } else {
                    Color::Green
                };

                // Calculate absolute coordinates dynamically based on current terminal size
                // ensuring nodes always scale and fit nicely inside the canvas bounds.
                let node_width = 24;
                let node_height = 5;
                
                let max_x = canvas_area.width.saturating_sub(node_width + 2);
                let max_y = canvas_area.height.saturating_sub(node_height + 2);

                let x = ((canvas_area.width as f32 * peer.x_ratio) as u16)
                    .clamp(canvas_area.x + 2, canvas_area.x + max_x);
                let y = ((canvas_area.height as f32 * peer.y_ratio) as u16)
                    .clamp(canvas_area.y + 1, canvas_area.y + max_y);

                let node_block = Block::default()
                    .title(peer.name.as_str())
                    .borders(Borders::ALL)
                    .style(Style::default().fg(border_color));
                
                let node_rect = Rect::new(x, y, node_width, node_height);
                let node_content = Paragraph::new(format!("Status: Active\nMsgs: {}", peer.messages.len()))
                    .block(node_block);
                
                f.render_widget(node_content, node_rect);

                // 3. Render Dynamic Adaptive Chat Bubble Popup
                if is_target {
                    let chat_width = 32;
                    let chat_height = 7;

                    // Intelligently check if popup fits on the right side of the node.
                    // If not, render it neatly on the left side instead!
                    let chat_x = if x + node_width + chat_width < canvas_area.x + canvas_area.width {
                        x + node_width + 2
                    } else {
                        x.saturating_sub(chat_width + 2)
                    };

                    let chat_y = y;

                    if chat_x + chat_width <= canvas_area.x + canvas_area.width && chat_y + chat_height <= canvas_area.y + canvas_area.height {
                        let popup_rect = Rect::new(chat_x, chat_y, chat_width, chat_height);
                        
                        f.render_widget(Clear, popup_rect);

                        let chat_block = Block::default()
                            .title(format!(" Chat Log "))
                            .borders(Borders::ALL)
                            .style(Style::default().fg(Color::Yellow));

                        let display_text = peer.messages
                            .iter()
                            .rev()
                            .take(3)
                            .cloned()
                            .collect::<Vec<_>>()
                            .into_iter()
                            .rev()
                            .collect::<Vec<_>>()
                            .join("\n");

                        let chat_paragraph = Paragraph::new(display_text)
                            .block(chat_block)
                            .wrap(ratatui::widgets::Wrap { trim: true });

                        f.render_widget(chat_paragraph, popup_rect);
                    }
                }
            }

            // 4. Render Fixed Bottom Text Input Bar
            let input_block = Block::default()
                .title(" Send Message to Active Peer ")
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::Green));

            let input_text = format!("> {}_", app.input_buffer);
            let input_paragraph = Paragraph::new(input_text).block(input_block);
            f.render_widget(input_paragraph, input_area);
        })?;

        // Event loop polling with KeyPress check to prevent double typing
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    app.handle_key(key.code);
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
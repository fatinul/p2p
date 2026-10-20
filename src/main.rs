use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::{Line as TextLine, Span},
    widgets::{
        canvas::{Canvas, Line},
        Block, Borders, Clear, Paragraph,
    },
    Terminal,
};
use std::{io, time::Duration};

struct Peer {
    name: String,
    x_ratio: f32,
    y_ratio: f32,
    messages: Vec<String>,
}

struct FlightAnimation {
    target_peer: usize,
    words: Vec<String>,
    progress: f32, // 0.0 to 1.0
}

struct App {
    should_quit: bool,
    peers: Vec<Peer>,
    selected_peer: usize,
    input_buffer: String,
    animations: Vec<FlightAnimation>,
}

impl App {
    fn new() -> Self {
        Self {
            should_quit: false,
            peers: vec![
                Peer {
                    name: "Node-Alpha (You)".to_string(),
                    x_ratio: 0.15,
                    y_ratio: 0.25,
                    messages: vec!["Local node initialized.".to_string()],
                },
                Peer {
                    name: "Node-Beta (Peer 1)".to_string(),
                    x_ratio: 0.50,
                    y_ratio: 0.60,
                    messages: vec!["Connected to network.".to_string()],
                },
                Peer {
                    name: "Node-Gamma (Peer 2)".to_string(),
                    x_ratio: 0.75,
                    y_ratio: 0.30,
                    messages: vec!["Syncing block headers...".to_string()],
                },
            ],
            selected_peer: 1,
            input_buffer: String::new(),
            animations: Vec::new(),
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
                    let words: Vec<String> = self
                        .input_buffer
                        .split_whitespace()
                        .map(|s| s.to_string())
                        .collect();

                    if !words.is_empty() {
                        self.animations.push(FlightAnimation {
                            target_peer: self.selected_peer,
                            words,
                            progress: 0.0,
                        });
                    }
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
        // Tick active flight animations smoothly
        app.animations.retain_mut(|anim| {
            anim.progress += 0.025;
            if anim.progress >= 1.0 {
                let full_msg = format!("Out: {} ✓", anim.words.join(" "));
                app.peers[anim.target_peer].messages.push(full_msg);
                false
            } else {
                true
            }
        });

        terminal.draw(|f| {
            let area = f.area();

            // Split screen vertically: Graph canvas on top, chat input at bottom
            let vertical_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(10), Constraint::Length(3)])
                .split(area);

            let canvas_area = vertical_chunks[0];
            let input_area = vertical_chunks[1];

            // Canvas border block
            let canvas_block = Block::default()
                .title(" P2P Network Graph [Tab: Switch Peer | Enter: Send | Esc: Quit] ")
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::Cyan));

            f.render_widget(canvas_block, canvas_area);

            // Compute node screen positions
            let node_width = 24u16;
            let node_height = 5u16;

            let mut node_positions = Vec::new();
            for peer in &app.peers {
                let max_x = canvas_area.width.saturating_sub(node_width + 2);
                let max_y = canvas_area.height.saturating_sub(node_height + 2);

                let x = ((canvas_area.width as f32 * peer.x_ratio) as u16)
                    .clamp(canvas_area.x + 2, canvas_area.x + max_x);
                let y = ((canvas_area.height as f32 * peer.y_ratio) as u16)
                    .clamp(canvas_area.y + 1, canvas_area.y + max_y);

                let center_x = x + (node_width / 2);
                let center_y = y + (node_height / 2);

                node_positions.push((x, y, center_x, center_y));
            }

            let local_center = (node_positions[0].2, node_positions[0].3);

            // 2. Render Lines and Traveling Text Words via Canvas
            let inner_canvas_area = canvas_area.inner(ratatui::layout::Margin {
                vertical: 1,
                horizontal: 1,
            });

            let canvas_height = inner_canvas_area.height;
            let active_target = app.selected_peer;
            
            // Clone lightweight data needed inside the canvas closure to avoid borrow checker errors
            let canvas_node_positions = node_positions.clone();
            let canvas_animations: Vec<(usize, Vec<String>, f32)> = app
                .animations
                .iter()
                .map(|a| (a.target_peer, a.words.clone(), a.progress))
                .collect();
            let total_peers = app.peers.len();

            let canvas = Canvas::default()
                .x_bounds([0.0, canvas_area.width as f64])
                .y_bounds([0.0, canvas_area.height as f64])
                .paint(move |ctx| {
                    // Draw base connecting lines
                    for i in 1..total_peers {
                        let is_target = i == active_target;
                        let line_color = if is_target { Color::Yellow } else { Color::DarkGray };
                        let peer_center = (canvas_node_positions[i].2, canvas_node_positions[i].3);

                        let x1 = local_center.0 as f64;
                        let y1 = (canvas_height - local_center.1) as f64;
                        let x2 = peer_center.0 as f64;
                        let y2 = (canvas_height - peer_center.1) as f64;

                        ctx.draw(&Line {
                            x1,
                            y1,
                            x2,
                            y2,
                            color: line_color,
                        });
                    }

                    // Draw flying text words along active flight paths
                    for (target_peer, words, progress) in &canvas_animations {
                        let peer_center = (canvas_node_positions[*target_peer].2, canvas_node_positions[*target_peer].3);
                        let x1 = local_center.0 as f64;
                        let y1 = (canvas_height - local_center.1) as f64;
                        let x2 = peer_center.0 as f64;
                        let y2 = (canvas_height - peer_center.1) as f64;

                        let current_x = x1 + (x2 - x1) * *progress as f64;
                        let current_y = y1 + (y2 - y1) * *progress as f64;

                        let display_msg = format!("📦 [{}]", words.join(" "));
                        let styled_line = TextLine::from(Span::styled(display_msg, Style::default().fg(Color::Yellow)));
                        ctx.print(current_x, current_y, styled_line);
                    }
                });

            f.render_widget(canvas, inner_canvas_area);

            // 3. Render Nodes & Line-Avoiding Chat Popups
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

                let (x, y, center_x, center_y) = node_positions[i];
                let node_rect = Rect::new(x, y, node_width, node_height);

                f.render_widget(Clear, node_rect);

                let node_block = Block::default()
                    .title(peer.name.as_str())
                    .borders(Borders::ALL)
                    .style(Style::default().fg(border_color));

                let node_content = Paragraph::new(format!(
                    "Status: Active\nMsgs: {}",
                    peer.messages.len()
                ))
                .block(node_block);

                f.render_widget(node_content, node_rect);

                // Render Chat Bubble safely avoiding the connecting line trajectory
                if is_target {
                    let chat_width = 34;
                    let chat_height = 7;

                    let chat_x = if center_x >= local_center.0 {
                        if y > node_height + 2 { x.saturating_sub(chat_width + 2) } else { x }
                    } else {
                        x + node_width + 2
                    };

                    let chat_y = if center_y < canvas_area.height / 2 {
                        y + node_height
                    } else {
                        y.saturating_sub(chat_height)
                    };

                    let chat_x = chat_x.clamp(canvas_area.x + 1, canvas_area.x + canvas_area.width.saturating_sub(chat_width + 1));
                    let chat_y = chat_y.clamp(canvas_area.y + 1, canvas_area.y + canvas_area.height.saturating_sub(chat_height + 1));

                    if chat_x + chat_width <= canvas_area.x + canvas_area.width
                        && chat_y + chat_height <= canvas_area.y + canvas_area.height
                    {
                        let popup_rect = Rect::new(chat_x, chat_y, chat_width, chat_height);

                        f.render_widget(Clear, popup_rect);

                        let chat_block = Block::default()
                            .title(" Chat Log ")
                            .borders(Borders::ALL)
                            .style(Style::default().fg(Color::Yellow));

                        let mut display_lines = peer
                            .messages
                            .iter()
                            .rev()
                            .take(2)
                            .cloned()
                            .collect::<Vec<_>>();
                        display_lines.reverse();

                        for anim in &app.animations {
                            if anim.target_peer == i {
                                let visible_word_count = ((anim.words.len() as f32) * anim.progress).ceil() as usize;
                                let partial_text = anim.words[..visible_word_count.min(anim.words.len())].join(" ");
                                display_lines.push(format!("Out: {} ⏳", partial_text));
                            }
                        }

                        let display_text = display_lines.join("\n");
                        let chat_paragraph = Paragraph::new(display_text)
                            .block(chat_block)
                            .wrap(ratatui::widgets::Wrap { trim: true });

                        f.render_widget(chat_paragraph, popup_rect);
                    }
                }
            }

            // 4. Render Fixed Bottom Text Input Bar
            let active_peer_name = &app.peers[app.selected_peer].name;
            let input_block = Block::default()
                .title(" Message Input ")
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::Green));

            let input_text = format!("[To: {}] > {}_", active_peer_name, app.input_buffer);
            let input_paragraph = Paragraph::new(input_text).block(input_block);
            f.render_widget(input_paragraph, input_area);
        })?;

        if event::poll(Duration::from_millis(30))? {
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
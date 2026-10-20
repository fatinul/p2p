# P2P Terminal Chat Room

A decentralized peer-to-peer chat application with a node-graph terminal UI built in Rust using Ratatui.

## Features
- **Decentralized P2P Networking:** Host or join rooms locally or over LAN[cite: 1].
- **Persistent Cryptographic Identity:** Automatically generates a unique cryptographic ID and avatar stored securely via SQLite (`database.db`)[cite: 1].
- **Node-Graph Terminal UI:** Visually interactive node mapping with live message animations and scrollable chat logs[cite: 1].
- **Conversation Isolation:** Database queries isolate chat histories per user UUID to prevent message conflicts.

---

## How to Test and Run

### 1. Download the Binary
Go to the **[Releases](../../releases)** page of this repository and download the appropriate executable for your operating system:
- **Windows:** `p2p_room.exe`
- **Linux:** `p2p_room_linux`
- **macOS:** `p2p_room_macos`

### 2. Running the App

Open your terminal in the folder where you downloaded the binary and run it:

- **Windows (Command Prompt / PowerShell):**
```cmd
p2p_room.exe
```

- **Linux / macOS**
```bash
chmod +x p2p_room_linux
./p2p_room_linux
```

3. Testing Multi-User Chat (Localhost)

1. Open Terminal Window A, launch the app, select [ Host Server ] by pressing Left/Right, and hit Enter to start the server. Note the address (e.g., 127.0.0.1:8001).
2. Open Terminal Window B, launch the app, leave it on [ Join Server ], enter 127.0.0.1:8001, type a different player name, and press Enter.
3. You will see both nodes pop up in a graph layout, allowing you to chat back and forth!


---

### How to Trigger a Release Build
To push a release and let GitHub automatically build your binaries:
1. Initialize git and commit your files:
   ```bash
   git init
   git add .
   git commit -m "Setup CI/CD release workflow"

    Link your local repo to GitHub and push:
    Bash

    git remote add origin <your-github-repo-url>
    git push -u origin main

    Create and push a version tag:
    Bash

    git tag v1.0.0
    git push origin v1.0.0

    Go to your GitHub repository under Actions, watch the build finish, and check your Releases tab to download the compiled binaries for all platforms!
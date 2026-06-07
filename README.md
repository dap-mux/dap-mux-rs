# dap-mux

Inspired by (https://github.com/dap-mux/dap-mux)[dap-mux].

dap-mux is a DAP proxy or multiplexer. It sits above your debug adapter and relays its input and output.
But why?? You might be asking. Because now the debug session is not locking you into just your IDE and the debugger.
Other clients can connect and help you visualize what is happening. You could connect a full IPython REPL to your
Python debug session. Or see a bread crumb trail of what you have been.

dap-mux is just a pass through. It listens for DAP on stdio or TCP and responds on stdio or TCP. That is it.
The work happens in the clients.

## Features

You can start the mux in several ways:
- your editor can start it and talk to it over stdio.
- you can start it in a terminal and talk to it over TCP.

There is a TUI available with `--ui` which shows you the connected clients, the TCP information, and
any logging. This is the recommended use when not initiated by your editor.

## Who This Is For

* Terminal-first developers using a DAP-capable editor who want IDE-quality debugging without leaving the terminal
* Data scientists who live in IPython and want visual source tracking while debugging
* Remote developers debugging over SSH where GUI IDEs are impractical

For me, personally, I hack in Helix which has lackluster DAP support. I would rather lean into UNIX and build
the smaller tools that make things work. This mux makes it possible to do that.

In theory a scripting capable client could join the conversation and replay some commands programmatically without
the arcane gdb/lldb syntax. Or it one could startup and slurp a bunch of data out, maybe before and after a patch is applied
to compare notes. Lots of options once the possibility of arbitrary tools is available.

For sure some of this is possible already by simply using lldb or pudb and not using an editor and DAP. However, a mux
opens the doors for experimentation. Automation in our tooling. A shared ecosystem the is above any one language.

## Installation & Configuration.

The usual cargo dance.

No configuration is required when launched in a terminal. If you want it to be started by your editor then you
would configure your editor to start `dap-mux` as the debugger and pass in the debugger info to `dap-mux`
which can either talk remotely to the debugger or spawn it and talk to it over stdio.

## Quick Start

This example uses Helix and debugpy. Any DAP-capable editor works — see [Editor Setup](#editor-setup).

debugpy must be available in the *target* environment:

```
pip install debugpy    # in your project's virtualenv
```

**1. Start the session in a terminal**

```
dap-mux --mux-port 5555 --ui -- python -m debugpy.adapter
```

dap-mux spawns debugpy, connects the multiplexer, and is ready for debugging:

**2. Set breakpoints in Helix, then connect**

Open script.py in Helix and set a breakpoint on the line you want to pause on (`<space>Gb` or your configured key). Then connect:

```
:debug-remote 127.0.0.1:5555 launch
```

When Helix connects it sends `configurationDone`, which starts the script. With no breakpoints, the script runs to completion before you can do anything.

Execution starts and pauses at your breakpoint. Helix highlights the current line.

**3. Listen in on that session **

Run `dap-tui localhost:5555` and you can watch the stack frames change as you step through the code.
OR
```
pip install dap-mux
```
and with the Python (https://github.com/dap-mux/dap-mux)[dap-mux] package installed you can fire up and IPython session and
```
%connect localhost:5555
```
and observe right there in the Python shell.

## Usage

You have choices.

Do you want to manage everything by hand in a terminal? Run with the `--ui` option and go for it.
Do you want to launch and manage the debugging in your editor? Add `dap-mux` to your editor's config
as a wrapper around the debugger you usually use. Can be stdio style in which case `dap-mux` will spawn
the debugger or TCP mode and connect to a debugger you already started.

If you want to connect other clients, and if you don't why use a mux??, remember to pass `--mux-port`
to tell the mux which port to open for listening.

## Editor Setup

The examples below show a Python configuration. But any debugger which speaks DAP can be used. Rust, Go,
Javascript/Typescript can all be used.

### Helix

Helix will look for a .helix directory in the root of whatever repo you are hacking in. You can put a locally
modified languages.toml in that directory.

```toml
[[language]]
name = "python"

[language.debugger]
name = "dap-mux"
transport = "stdio"
# This is usually the full path.
command = "/path/to/dap-mux"

# transport = "stdio" means Helix drives dap-mux over its own stdin/stdout, so
# dap-mux must run with --stdio otherwise it defaults to a standalone TCP host
# and never speaks DAP on stdio which will confuse Helix. --mux-port additionally opens a TCP listener
# for extra clients. The adapter aka debugger is defined with --adapter string 
# so debugpy's own `-m debugpy.adapter` stays attached to it.
# The "launch" command below will send the correct program to the adapter.
args = [
    "--stdio",
    "--mux-port", "5544",
    "--adapter", "/path/to/.venv/bin/python -m debugpy.adapter",
]

[[language.debugger.templates]]
name = "launch"
request = "launch"
# program is hardcoded so `:debug-remote <addr> launch` needs no argument.
completion = []
args = { mode = "debug", program = "program.py", console = "internalConsole" }

[[language.debugger.templates]]
name = "attach"
request = "attach"
completion = []
args = {}
```

Connect to a running dap-mux with `:debug-remote host:port attach`.

### Other editors

Any editor with a DAP client works. Configure it to connect to `127.0.0.1:<mux-port>` as an existing DAP server — dap-mux speaks standard DAP, no special configuration needed.

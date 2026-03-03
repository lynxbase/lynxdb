---
sidebar_position: 13
title: Shell Completion
description: Set up shell completion for LynxDB CLI in Bash, Zsh, Fish, and PowerShell.
---

# Shell Completion

Generate shell completion scripts for tab-completing LynxDB commands, flags, and arguments.

```
lynxdb completion [bash|zsh|fish|powershell]
```

## Bash

```bash
# Add to current session
source <(lynxdb completion bash)

# Persist across sessions
lynxdb completion bash >> ~/.bashrc
```

After adding to `~/.bashrc`, restart your shell or run `source ~/.bashrc`.

## Zsh

```bash
# Add to current session
source <(lynxdb completion zsh)

# Persist across sessions
lynxdb completion zsh >> ~/.zshrc
```

If you use a completions directory:

```bash
lynxdb completion zsh > "${fpath[1]}/_lynxdb"
```

After adding, restart your shell or run `source ~/.zshrc`.

## Fish

```bash
lynxdb completion fish > ~/.config/fish/completions/lynxdb.fish
```

Fish loads completions automatically from the completions directory.

## PowerShell

```powershell
# Add to current session
lynxdb completion powershell | Out-String | Invoke-Expression

# Persist across sessions (add to your PowerShell profile)
lynxdb completion powershell >> $PROFILE
```

## What Gets Completed

The completion script provides tab-completion for:

- All top-level commands (`query`, `ingest`, `server`, `tail`, `mv`, etc.)
- Subcommands (`mv create`, `mv list`, `config get`, `alerts test`, etc.)
- Flags and their values (e.g., `--format` completes to `json`, `table`, `csv`, etc.)
- Config keys for `config get` and `config set`
- Saved query names for `saved run` and `saved delete`
- Materialized view names for `mv status`, `mv drop`, `mv pause`, `mv resume`

## Verifying Completion

After installation, verify that completion is working:

```bash
lynxdb <TAB><TAB>
# Should list all available commands

lynxdb query --format <TAB><TAB>
# Should list: auto json ndjson table csv tsv raw

lynxdb mv <TAB><TAB>
# Should list: create list status drop pause resume
```

## Troubleshooting

If completions are not working:

1. Run `lynxdb doctor` to check if shell completion is detected
2. Ensure your shell is sourcing the completion file
3. For Zsh, ensure `compinit` is called in your `.zshrc`
4. For Bash, ensure `bash-completion` is installed (`apt install bash-completion` or `brew install bash-completion`)

## See Also

- [CLI Overview](/docs/cli/overview) for all available commands
- [config](/docs/cli/config-cmd) for the `doctor` command that checks completion setup

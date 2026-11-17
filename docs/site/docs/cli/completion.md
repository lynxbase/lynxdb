---
sidebar_position: 13
title: Shell Completion
description: Tab completion for the LynxDB CLI in Bash, Zsh, Fish and PowerShell.
---

# Shell Completion

```
lynxdb completion bash|zsh|fish|powershell
```

Prints a completion script for the shell named in the argument.

## Bash

```bash
# Current session
source <(lynxdb completion bash)

# Every session
lynxdb completion bash >> ~/.bashrc
```

Bash completion needs the `bash-completion` package (`apt install bash-completion`, `brew install bash-completion@2`).

## Zsh

```bash
# Current session
source <(lynxdb completion zsh)

# Every session, from a directory in $fpath
lynxdb completion zsh > "${fpath[1]}/_lynxdb"
```

`compinit` must run in `~/.zshrc`. Start a new shell after installing.

## Fish

```bash
lynxdb completion fish > ~/.config/fish/completions/lynxdb.fish
```

## PowerShell

```powershell
# Current session
lynxdb completion powershell | Out-String | Invoke-Expression

# Every session
lynxdb completion powershell >> $PROFILE
```

## What completes

- command and subcommand names, such as `lynxdb config <TAB>`
- flag names, such as `lynxdb query --<TAB>`
- config keys for `config get` and `config set`
- field names for `lynxdb fields <TAB>`, fetched from the server
- job IDs for `lynxdb jobs <TAB>`, fetched from the server

Field names and job IDs come from the server named by `--server` with a 2-second timeout; when it does not answer, nothing is offered. Query text and flag values are not completed.

## See also

- [CLI overview](./overview.md)

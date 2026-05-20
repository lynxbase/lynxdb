Output demo.gif

Require lynxdb
Require zsh

Set Shell "zsh"
Set FontSize 20
Set Width 1500
Set Height 860
Set Framerate 30
Set TypingSpeed 28ms
Set Padding 18
Set Margin 28
Set MarginFill "#111827"
Set BorderRadius 8
Set WindowBar Colorful
Set Theme "Catppuccin Frappe"
Set CursorBlink false
Set LoopOffset 18%

Env lynxdb_SERVER "http://127.0.0.1:3100"

Hide
Type "ZSH_HIGHLIGHT_HIGHLIGHTERS=(main pattern)"
Enter
Type "for f in /opt/homebrew/share/zsh-syntax-highlighting/zsh-syntax-highlighting.zsh /usr/local/share/zsh-syntax-highlighting/zsh-syntax-highlighting.zsh; do [[ -r $f ]] && source $f && break; done"
Enter
Type "ZSH_HIGHLIGHT_STYLES[command]='fg=#38bdf8,bold'"
Enter
Type "ZSH_HIGHLIGHT_STYLES[single-quoted-argument]='fg=#a6e3a1'"
Enter
Type "ZSH_HIGHLIGHT_STYLES[double-quoted-argument]='fg=#a6e3a1'"
Enter
Type "typeset -gA ZSH_HIGHLIGHT_PATTERNS"
Enter
Type "ZSH_HIGHLIGHT_PATTERNS=( '(#i)(FROM|parse|where|stats|sort|head)' 'fg=#38bdf8,bold' '((#s)|[[:space:]|(),])(#i)(by|as|or|and)((#e)|[[:space:]|(),])' 'fg=#f472b6,bold' '(#i)(count|json|combined|cef)' 'fg=#fde68a,bold' '>=|=|-hits|-count' 'fg=#f472b6,bold' )"
Enter
Type "PS1='lynxdb-demo $ '"
Enter
Type "clear"
Enter
Show

Type "lynxdb status"
Enter
Wait+Screen /healthy/
Sleep 2s

Ctrl+L
Type "lynxdb shippers"
Enter
Wait+Screen /filebeat/
Sleep 2s

Ctrl+L
Type "lynxdb indexes"
Enter
Wait+Screen /23 indexes/
Sleep 2s

Ctrl+L
Type `lynxdb query 'FROM app-json | parse json(message) | where level="ERROR" or level="FATAL" | stats count() by service, level | sort -count | head 8' --since 1h`
Enter
Wait+Screen /payment-svc|order-svc|user-svc/
Sleep 3s

Ctrl+L
Type "lynxdb query 'FROM nginx-access | parse combined(message) | where status>=500 | stats count() as hits by uri | sort -hits | head 8' --since 1h"
Enter
Wait+Screen /\/admin|\/index.html|\/api/
Sleep 3s

Ctrl+L
Type `lynxdb query 'FROM cef-security | parse cef(message) | where severity="Critical" or severity="High" | stats count() as hits by severity, act | sort -hits | head 8' --since 1h`
Enter
Wait+Screen /Critical/
Sleep 3s

Ctrl+L
Type "lynxdb query --explain 'FROM nginx-access | parse combined(message) | where status>=500 | stats count() as hits by uri | sort -hits | head 8'"
Enter
Wait+Screen /TopK heap optimization/
Sleep 4s

Ctrl+L
Type "lynxdb shell --since 1h"
Enter
Wait+Screen /lynxdb shell/
Sleep 1s

Type `FROM app-json | parse json(message) | stats count() by level | sort -count | head 5`
Enter
Wait+Screen /FATAL|ERROR|WARN/
Sleep 2s

Ctrl+L
Type `FROM nginx-access | parse combined(message) | where status>=500 | stats count() by status | sort status`
Enter
Wait+Screen /500|502|503|504/
Sleep 3s

Ctrl+D
Wait+Screen /lynxdb-demo/
Sleep 1s

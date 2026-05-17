package ui

import (
	"fmt"
	"io"
	"strings"
	"time"
)

type LynxMood string

const (
	LynxAlert LynxMood = "alert"
	LynxSad   LynxMood = "sad"
)

func LynxFrame(mood LynxMood) string {
	frames := LynxFrames(mood)
	if len(frames) == 0 {
		return ""
	}

	return frames[len(frames)-1]
}

func LynxFrames(mood LynxMood) []string {
	switch mood {
	case LynxSad:
		return []string{
			"  /\\_/\\\n ( ._. )\n  > _ <",
			"  /\\_/\\\n ( ;_; )\n  > _ <",
			"  /\\_/\\\n ( u_u )\n  > _ <",
		}
	default:
		return []string{
			"  /\\_/\\\n ( -.- )\n  > ^ <",
			"  /\\_/\\\n ( o.- )\n  > ^ <",
			"  /\\_/\\\n ( o.o )\n  > ^ <",
			"  /\\_/\\\n ( o.o )\n  > v <",
			"  /\\_/\\\n ( o.o )\n  > ^ <",
		}
	}
}

func RenderLynxFrame(t *Theme, frame string) string {
	if t == nil {
		t = Stdout
	}

	lines := strings.Split(frame, "\n")
	if len(lines) != 3 {
		return frame + "\n"
	}

	return "  " + t.Success.Render(lines[0]) + "\n" +
		"  " + t.Info.Render(lines[1]) + "\n" +
		"  " + t.Accent.Render(lines[2]) + "\n"
}

func WriteLynxMark(w io.Writer, t *Theme, mood LynxMood, animate bool) {
	if w == nil {
		return
	}

	frames := LynxFrames(mood)
	if len(frames) == 0 {
		return
	}

	if !animate {
		fmt.Fprint(w, RenderLynxFrame(t, frames[len(frames)-1]))
		return
	}

	fmt.Fprintln(w)
	for i, frame := range frames {
		if i > 0 {
			fmt.Fprint(w, "\033[3A")
		}
		fmt.Fprint(w, RenderLynxFrame(t, frame))
		time.Sleep(65 * time.Millisecond)
	}
}

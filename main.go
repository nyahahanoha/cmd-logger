package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/charmbracelet/x/term"
	"github.com/creack/pty"
	"github.com/hinshun/vt10x"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
)

const (
	commandName    = "cmd-logger"
	versionFlag    = "version"
	dirFlag        = "dir"
	defaultLogDir  = "logs"
	fileFlag       = "file"
	defaultLogfile = "out"

	defaultTimeFormat = "20060102_150405"

	// Screen logging: the raw PTY stream is fed to a virtual terminal and
	// the log is written as readable plain text (see screenLogger).
	snapshotInterval   = 100 * time.Millisecond // how often to check for a new frame
	snapshotSettle     = 200 * time.Millisecond // quiet period before a frame is captured
	snapshotMaxDelay   = time.Second            // capture at least this often while output keeps flowing
	snapshotTimeFormat = "2006-01-02 15:04:05"

	// feedStep is the chunk size fed to the virtual terminal at a time on
	// the normal screen, so scrolling can be tracked before lines are lost.
	feedStep = 32
)

// screenLogger renders the PTY output stream on a virtual terminal and
// writes a readable text log:
//
//   - On the normal screen, lines are written as they scroll off the top,
//     so plain line output (build logs, command output, shell sessions)
//     is preserved in full, in order.
//   - On the alternate screen (watch, vim, less, ...), timestamped
//     snapshots of the visible screen are written instead, so full-screen
//     commands leave a readable log rather than raw escape sequences.
type screenLogger struct {
	mu      sync.Mutex
	file    *os.File
	vt      vt10x.Terminal
	pending []byte // trailing bytes of an incomplete UTF-8 rune

	alt bool // the child is using the alternate screen

	// Alternate screen: frame snapshots.
	lastFrame string
	dirty     bool
	lastRecv  time.Time
	lastWrite time.Time

	// Normal screen: stream of scrolled-off lines.
	prev     []string // last sampled screen lines
	prevY    int      // cursor row at the time prev was sampled
	prevInit bool
	emitted  int // lines at the top of prev already written to the log

	stepNewlines int // newlines fed since the previous sample
}

func newScreenLogger(file *os.File, cols, rows int) *screenLogger {
	if cols <= 0 {
		cols = 80
	}
	if rows <= 0 {
		rows = 24
	}
	return &screenLogger{
		file: file,
		vt:   vt10x.New(vt10x.WithSize(cols, rows)),
	}
}

// Feed consumes raw PTY output.
func (s *screenLogger) Feed(p []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data := append(s.pending, p...)
	for len(data) > 0 {
		step := len(data)
		if !s.inAltLocked() {
			// On the normal screen, feed at most one completed line
			// at a time and sample after each: a line that is both
			// completed and scrolled off between two samples would
			// never appear in either of them and would be lost.
			if step > feedStep {
				step = feedStep
			}
			if i := bytes.IndexByte(data[:step], '\n'); i >= 0 {
				step = i + 1
			}
		}
		n, err := s.vt.Write(data[:step])
		if err != nil {
			log.Printf("failed to write to virtual terminal, err=%v", err)
		}
		if n == 0 {
			// Incomplete UTF-8 rune at the end; keep the bytes for the
			// next Feed.
			break
		}
		s.stepNewlines += bytes.Count(data[:n], []byte{'\n'})
		data = data[n:]
		s.sampleLocked()
	}
	s.pending = data

	s.lastRecv = time.Now()
}

// Resize resizes the virtual terminal to follow the real terminal.
func (s *screenLogger) Resize(cols, rows int) {
	if cols <= 0 || rows <= 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	c, r := s.vt.Size()
	if c == cols && r == rows {
		return
	}
	s.vt.Resize(cols, rows)

	if s.alt {
		s.dirty = true
		s.lastRecv = time.Now()
		return
	}
	// Reflow makes stream tracking unreliable: flush what is on screen
	// and start a new baseline.
	s.flushStreamLocked()
	s.prev = s.screenLinesLocked()
	s.prevY = s.vt.Cursor().Y
	s.prevInit = true
	s.emitted = len(s.prev)
}

func (s *screenLogger) inAltLocked() bool {
	return s.vt.Mode()&vt10x.ModeAltScreen != 0
}

// screenLinesLocked returns the visible screen as right-trimmed lines,
// with trailing blank lines dropped.
func (s *screenLogger) screenLinesLocked() []string {
	lines := strings.Split(s.vt.String(), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t\r")
	}
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// sampleLocked updates the log after new output was fed to the terminal.
func (s *screenLogger) sampleLocked() {
	if alt := s.inAltLocked(); alt != s.alt {
		if alt {
			// Entering the alternate screen: flush the normal screen
			// lines that have not been written yet.
			s.flushStreamLocked()
			s.dirty = true
			s.lastRecv = time.Now()
		} else {
			// Back on the normal screen: its content was restored and
			// has already been logged.
			s.prev = s.screenLinesLocked()
			s.prevY = s.vt.Cursor().Y
			s.prevInit = true
			s.emitted = len(s.prev)
		}
		s.alt = alt
		return
	}

	if s.alt {
		s.dirty = true
		s.lastRecv = time.Now()
		return
	}

	cur := s.screenLinesLocked()
	curY := s.vt.Cursor().Y
	// Lines can only scroll off when new lines are completed, so the
	// newlines fed since the previous sample bound the scroll amount.
	// This is what tells a genuine "everything scrolled away" apart
	// from an in-place redraw that replaced the whole screen.
	maxScroll := s.stepNewlines
	s.stepNewlines = 0
	if !s.prevInit {
		s.prev = cur
		s.prevY = curY
		s.prevInit = true
		s.emitted = 0
		return
	}
	if equalLines(cur, s.prev) {
		s.prevY = curY
		return
	}

	body := completeLines(s.prev, s.prevY)
	if k, ok := scrollAmount(s.prev, cur, body, maxScroll); ok {
		if k > s.emitted {
			s.writeLinesLocked(s.prev[s.emitted:k])
		}
		s.emitted -= k
		if s.emitted < 0 {
			s.emitted = 0
		}
		s.prev = cur
		s.prevY = curY
		return
	}

	// Growth (complete lines unchanged, the last line may still be
	// extended) or a change confined to the last line (typing at a
	// prompt, a progress bar): nothing has scrolled off, and the last
	// line is still being edited. It is logged once it scrolls off or
	// at the end.
	if isGrowth(s.prev, cur, body) || lastLineOnlyChange(s.prev, cur) {
		s.prev = cur
		s.prevY = curY
		return
	}

	// Anything else (in-place redraws, clears): keep a framed snapshot.
	s.writeFrameLocked(cur)
	s.prev = cur
	s.prevY = curY
	s.emitted = len(cur)
}

// completeLines returns how many of the sampled lines are known to be
// complete. The cursor sits on the line still being written; when it
// has moved below the last content line (the usual case right after a
// newline), every line is complete. Otherwise the last line is
// provisional: it may be extended by the next output, so comparisons
// must not rely on it.
func completeLines(lines []string, cursorY int) int {
	if cursorY >= len(lines) {
		return len(lines)
	}
	if len(lines) == 0 {
		return 0
	}
	return len(lines) - 1
}

// isGrowth reports whether cur is prev with more lines appended: all
// complete lines of prev are unchanged at the top of cur (the
// provisional last line may have been extended since it was sampled).
func isGrowth(prev, cur []string, body int) bool {
	if len(cur) < body {
		return false
	}
	if body == len(prev) && len(cur) < len(prev) {
		return false
	}
	return equalLines(prev[:body], cur[:body])
}

// maybeSnapshot writes a frame on the alternate screen if the screen
// changed and output settled. Called periodically.
func (s *screenLogger) maybeSnapshot() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.alt || !s.dirty {
		return
	}

	frame := strings.Join(s.screenLinesLocked(), "\n")
	if frame == "" || frame == s.lastFrame {
		s.dirty = false
		return
	}

	now := time.Now()
	if now.Sub(s.lastRecv) < snapshotSettle && now.Sub(s.lastWrite) < snapshotMaxDelay {
		// Output is still flowing; wait for it to settle.
		return
	}
	s.writeFrameLocked(s.screenLinesLocked())
	s.dirty = false
	s.lastWrite = now
}

// flush writes whatever is still on screen when the command ends.
func (s *screenLogger) flush() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.alt {
		s.writeFrameLocked(s.screenLinesLocked())
		return
	}
	s.flushStreamLocked()
}

// flushStreamLocked writes the normal-screen lines that have not been
// written yet.
func (s *screenLogger) flushStreamLocked() {
	if s.prevInit && s.emitted < len(s.prev) {
		s.writeLinesLocked(s.prev[s.emitted:])
		s.emitted = len(s.prev)
	}
}

func (s *screenLogger) writeLinesLocked(lines []string) {
	for _, line := range lines {
		if _, err := fmt.Fprintln(s.file, line); err != nil {
			log.Printf("failed to write log line, err=%v", err)
		}
	}
}

func (s *screenLogger) writeFrameLocked(lines []string) {
	frame := strings.Join(lines, "\n")
	if frame == "" || frame == s.lastFrame {
		return
	}
	if _, err := fmt.Fprintf(s.file, "===== %s =====\n%s\n", time.Now().Format(snapshotTimeFormat), frame); err != nil {
		log.Printf("failed to write snapshot, err=%v", err)
	}
	s.lastFrame = frame
}

func equalLines(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// scrollAmount returns how many lines prev scrolled up to become cur:
// prev[k:body] must be a prefix of cur, where body is the number of
// complete lines in prev (see completeLines). The provisional last
// line is excluded from the match: sampled in the middle of a step,
// it may have been completed since, which would poison an exact
// comparison and make a small scroll look like a full screen
// replacement.
//
// k is at most maxScroll: only completed new lines can push lines off
// the top, so a larger k would mean the screen was redrawn in place
// rather than scrolled.
func scrollAmount(prev, cur []string, body int, maxScroll int) (int, bool) {
	maxK := body
	if maxScroll < maxK {
		maxK = maxScroll
	}
	for k := 0; k <= maxK; k++ {
		rest := prev[k:body]
		if len(rest) > len(cur) {
			continue
		}
		if equalLines(rest, cur[:len(rest)]) {
			return k, true
		}
	}
	return 0, false
}

// lastLineOnlyChange reports whether prev and cur differ only in their
// last line.
func lastLineOnlyChange(prev, cur []string) bool {
	if len(prev) == 0 || len(prev) != len(cur) {
		return false
	}
	return equalLines(prev[:len(prev)-1], cur[:len(cur)-1])
}

var (
	ErrNotFoundCmd  = errors.New("a command is required")
	ErrEmptyLogFile = errors.New("a log file is required")
)

func record(_ context.Context, file *os.File, argv []string) error {
	if len(argv) == 0 {
		return ErrNotFoundCmd
	}

	cmd := exec.Command(argv[0], argv[1:]...)

	cols, rows, _ := term.GetSize(os.Stdin.Fd())

	ptmx, err := pty.Start(cmd)
	if err != nil {
		log.Printf("failed to start command, err=%v", err)
		return err
	}
	defer ptmx.Close()

	logger := newScreenLogger(file, cols, rows)

	// Terminal raw mode
	oldState, err := term.MakeRaw(os.Stdin.Fd())
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		log.Printf("failed to change terminal raw mode, err=%v", err)
		return err
	}
	defer term.Restore(os.Stdin.Fd(), oldState)

	ch := make(chan os.Signal, 1)

	signal.Notify(ch, syscall.SIGWINCH)
	defer func() {
		signal.Stop(ch)
		close(ch)
	}()

	// Window resize
	go func() {
		for range ch {
			if err := pty.InheritSize(os.Stdin, ptmx); err != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				log.Printf("failed to resize pty, err=%v", err)
			}
			if w, h, err := term.GetSize(os.Stdin.Fd()); err == nil {
				logger.Resize(w, h)
			}
		}
	}()

	// Apply the initial size (PTY and virtual terminal) right away,
	// so full-screen commands start with the real terminal size.
	ch <- syscall.SIGWINCH

	// Stdin -> PTY
	go func() {
		if _, err := io.Copy(ptmx, os.Stdin); err != nil {
			log.Printf("faild to copy stdin pty, err=%v", err)
		}
	}()

	var eg errgroup.Group

	// PTY -> Stdout, Log (as readable text)
	eg.Go(func() error {
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(snapshotInterval)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					logger.maybeSnapshot()
				}
			}
		}()

		var copyErr error
		buf := make([]byte, 1024)
		for {
			n, readErr := ptmx.Read(buf)
			if n > 0 {
				chunk := buf[:n]
				if _, err := os.Stdout.Write(chunk); err != nil {
					log.Printf("failed to copy pty stdout, err=%v", err)
				}
				logger.Feed(chunk)
			}
			if readErr != nil {
				if !errors.Is(readErr, syscall.EIO) && !errors.Is(readErr, io.EOF) {
					log.Printf("faild to copy pty stdout and logfile, err=%v", readErr)
					copyErr = readErr
				}
				break
			}
		}

		close(done)
		wg.Wait()
		logger.flush()

		return copyErr
	})

	// wait to finish command
	eg.Go(func() error {
		if err := cmd.Wait(); err != nil {
			log.Printf("failed to finish command, err=%v", err)
			return err
		}
		return nil
	})

	groupErr := eg.Wait()

	return groupErr
}

func createLogFile(_ context.Context, logDirPath, logFileName string) (*os.File, error) {
	err := os.MkdirAll(logDirPath, 0755)
	if err != nil {
		log.Printf("failed to create log dir, err=%v", err)
		return nil, err
	}
	tempFile, err := os.CreateTemp(
		logDirPath,
		fmt.Sprintf("%s-%s-*.log", logFileName, time.Now().Format(defaultTimeFormat)),
	)
	if err != nil {
		log.Printf("failed to create tempfile, err=%v", err)
		return nil, err
	}

	return tempFile, nil
}

func main() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:  versionFlag,
		Usage: "print only the version",
	}
	cmd := &cli.Command{
		Name:         commandName,
		Usage:        "Logging of command input and output",
		StopOnNthArg: new(1),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  dirFlag,
				Value: defaultLogDir,
				Usage: "log dir",
			},
			&cli.StringFlag{
				Name:  fileFlag,
				Value: defaultLogfile,
				Usage: "log file name",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			var argv []string
			if cmd.NArg() == 0 {
				return ErrNotFoundCmd
			} else {
				argv = cmd.Args().Slice()
			}

			file, err := createLogFile(ctx, cmd.String(dirFlag), cmd.String(fileFlag))
			if err != nil {
				log.Printf("failed to create log file, err=%v", err)
			}

			if err := record(ctx, file, argv); err != nil {
				log.Printf("failed to logging, err=%v", err)
				return err
			}
			return nil
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}

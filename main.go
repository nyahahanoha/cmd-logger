package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/charmbracelet/x/term"
	"github.com/creack/pty"
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
)

var (
	ErrNotFoundCmd  = errors.New("a command is required")
	ErrEmptyLogFile = errors.New("a log file is required")
)

func record(_ context.Context, file *os.File, argv []string) error {
	if len(argv) == 0 {
		return ErrNotFoundCmd
	}

	cmd := exec.Command(argv[0], argv[1:]...)

	ptmx, err := pty.Start(cmd)
	if err != nil {
		log.Printf("failed to start command, err=%v", err)
		return err
	}
	defer ptmx.Close()

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
		}
	}()

	// Initial resize
	ch <- syscall.SIGWINCH

	// Stdin -> PTY
	go func() {
		if _, err := io.Copy(ptmx, os.Stdin); err != nil {
			log.Printf("faild to copy stdin pty, err=%v", err)
		}
	}()

	var eg errgroup.Group

	// PTY -> Stdout, Log
	eg.Go(func() error {
		writer := bufio.NewWriter(file)

		defer func() {
			if err := writer.Flush(); err != nil {
				log.Printf("failed to flush buffer, err=%v", err)
			}
		}()

		if _, err := io.Copy(io.MultiWriter(os.Stdout, writer), ptmx); err != nil && !errors.Is(err, syscall.EIO) {
			log.Printf("faild to copy pty stdout and logfile, err=%v", err)
			return err
		}
		return nil
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

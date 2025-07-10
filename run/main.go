package run

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/dottedmag/limestone/llog"
	"github.com/dottedmag/must"
	"github.com/dottedmag/parallel"
	"github.com/spf13/pflag"
)

var fs = pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)

func init() {
	fs.ParseErrorsWhitelist.UnknownFlags = true
	fs.BoolP("verbose", "v", false, "Enable verbose (debug level) messages")
	// Hide usage while parsing the command line here, will be covered by a regular command line parsing.
	fs.Usage = func() {}

	// Add options help to the main command-line parser.
	pflag.CommandLine.AddFlagSet(fs)
}

// Tool runs the top-level task of your program, watching for signals.
//
// The context passed to the task will contain a logger and an ID generator.
//
// If an interruption or termination signal arrives, the context passed to the
// task will be closed.
//
// Tool does not return. It exits with code 0 if the task returns nil, and
// with code 1 if the task returns an error.
//
// Any defer handlers installed before calling Main are ignored. For this
// reason, it is recommended that most or all your main code is inside the task.
//
// Simple example:
//
//	func main() {
//	    pflag.Parse()
//	    srv := service.New(...)
//	    run.Tool(srv.Run)
//	}
//
// Medium-complexity example:
//
//	func main() {
//	    pflag.Parse()
//	    run.Tool(func(ctx context.Context) error {
//	        if err := Step1(ctx); err != nil {
//	            return err
//	        }
//	        if err := Step2(ctx); err != nil {
//	            return err
//	        }
//	        return nil
//	    })
//	}
//
// Complex example:
//
//	func main() {
//	    pflag.Parse()
//	    run.Tool(func(ctx context.Context) error {
//	        return parallel.Run(func(ctx context.Context, spawn SpawnFn) {
//	            s1, err := service1.New(...)
//	            if err != nil {
//	                return err
//	            }
//
//	            s2, err := service2.New(...)
//	            if err != nil {
//	                return err
//	            }
//
//	            if err := s1.HeavyInit(ctx); err != nil {
//	                return err
//	            }
//
//	            spawn("service1", parallel.Fail, s1.Run)
//	            spawn("serivce2", parallel.Fail, s2.Run)
//	            return nil
//	        })
//	    })
//	}
func Tool(task func(ctx context.Context) error) {
	// os.Exit doesn't run deferred functions, so we'll call it in the first
	// defer which runs last
	var err error
	defer func() {
		var wec WithExitCode
		if errors.As(err, &wec) {
			os.Exit(wec.ExitCode())
		}
		if err != nil {
			os.Exit(1)
		}
	}()

	ctx := rootContext()

	err = parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("main", parallel.Exit, func(ctx context.Context) error {
			return task(ctx)
		})
		spawn("signals", parallel.Exit, handleSignals)
		return nil
	})
	if err != nil {
		llog.MustGet(ctx).Error("Error", llog.Error(err))
	}
}

// Server runs the top-level task of your program similar To Tool.
//
// The difference is in signal handling: if the top-level task exits with
// (possibly wrapped) context.Canceled while handling the signal, the program
// exits with code 0.
//
// Note that any other error returned during signal handling is still considered
// an error and makes Server exit with code 1.
func Server(task func(ctx context.Context) error) {
	Tool(func(ctx context.Context) error {
		err := task(ctx)
		if errors.Is(err, ctx.Err()) {
			return nil
		}
		return err
	})
}

// WithExitCode is an optional interface that can be implemented by an error.
//
// When a (possibly wrapped) error implementing WithExitCode reaches the top
// level, the value returned by the ExitCode method becomes the exit code of the
// process. The default exit code for other errors is 1.
type WithExitCode interface {
	ExitCode() int
}

func rootContext() context.Context {
	level := slog.LevelInfo
	if fs.Lookup("verbose").Changed && must.OK1(fs.GetBool("verbose")) {
		level = slog.LevelDebug
	}
	return llog.With(context.Background(), slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})))
}

// Command versiondemo reproduces cadence-workflow/cadence#8783 on a local Cadence server.
//
// It runs a workflow that calls GetVersion, then waits for a signal. Starting it on a worker that
// supports version 2 records version 2 in the history. Signalling it while only a worker that
// supports version 1 is running makes that old worker hit a version mismatch, which is what happens
// during a rolling deploy.
//
// Usage (from the root of the cadence-go-client checkout):
//
//	go run ./versiondemo worker -max 2   # new worker
//	go run ./versiondemo worker -max 1   # old worker
//	go run ./versiondemo start
//	go run ./versiondemo signal
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/zap"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
)

const (
	hostPort     = "127.0.0.1:7933"
	serviceName  = "cadence-frontend"
	domain       = "version-demo"
	taskList     = "version-demo"
	workflowID   = "version-demo"
	workflowName = "VersionDemoWorkflow"
	signalName   = "continue"
	changeID     = "demo-change"
)

// maxVersion is the highest version this worker's code supports. It stands in for "old code"
// (1) and "new code" (2).
var maxVersion workflow.Version = 1

func demoWorkflow(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)
	v := workflow.GetVersion(ctx, changeID, workflow.DefaultVersion, maxVersion)
	logger.Info("Workflow is using version", zap.Int("version", int(v)))

	workflow.GetSignalChannel(ctx, signalName).Receive(ctx, nil)
	logger.Info("Workflow received the signal and is finishing")
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: versiondemo worker -max N | start | signal")
		os.Exit(2)
	}

	service, stop := newService()
	defer stop()

	switch os.Args[1] {
	case "worker":
		fs := flag.NewFlagSet("worker", flag.ExitOnError)
		maxFlag := fs.Int("max", 1, "highest version this worker supports")
		_ = fs.Parse(os.Args[2:])
		maxVersion = workflow.Version(*maxFlag)
		runWorker(service)
	case "register":
		registerDomain(service)
	case "start":
		start(service)
	case "signal":
		send(service)
	default:
		fmt.Println("unknown command:", os.Args[1])
		os.Exit(2)
	}
}

func newService() (workflowserviceclient.Interface, func()) {
	transport, err := tchannel.NewTransport(tchannel.ServiceName("version-demo"))
	check(err)
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "version-demo",
		Outbounds: yarpc.Outbounds{
			serviceName: {Unary: transport.NewSingleOutbound(hostPort)},
		},
	})
	check(dispatcher.Start())
	return workflowserviceclient.New(dispatcher.ClientConfig(serviceName)), func() { _ = dispatcher.Stop() }
}

// registerDomain creates the demo domain, retrying for a few minutes while the server starts up.
func registerDomain(service workflowserviceclient.Interface) {
	retention := int32(1)
	name := domain
	var err error
	for attempt := 0; attempt < 60; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = client.NewDomainClient(service, &client.Options{}).Register(ctx, &shared.RegisterDomainRequest{
			Name:                                   &name,
			WorkflowExecutionRetentionPeriodInDays: &retention,
		})
		cancel()
		var exists *shared.DomainAlreadyExistsError
		if err == nil || errors.As(err, &exists) {
			return
		}
		fmt.Println("waiting for the Cadence server:", err)
		time.Sleep(5 * time.Second)
	}
	check(err)
}

func runWorker(service workflowserviceclient.Interface) {
	registerDomain(service)

	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	logConfig.DisableStacktrace = true
	logger, err := logConfig.Build()
	check(err)

	scope := tally.NewTestScope("", nil)
	w, err := worker.NewV2(service, domain, taskList, worker.Options{
		Logger:       logger,
		MetricsScope: scope,
	})
	check(err)
	w.RegisterWorkflowWithOptions(demoWorkflow, workflow.RegisterOptions{Name: workflowName})
	check(w.Start())
	fmt.Printf("\n>>> Worker running, supports versions up to %d. Press Ctrl+C to stop.\n\n", maxVersion)

	go printMetrics(scope)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)
	<-done
	w.Stop()
}

// printMetrics prints the two counters this demo is about every few seconds.
func printMetrics(scope tally.TestScope) {
	for range time.Tick(5 * time.Second) {
		var lines []string
		for _, c := range scope.Snapshot().Counters() {
			if !strings.Contains(c.Name(), "decision-task-panic") && !strings.Contains(c.Name(), "version-mismatch") {
				continue
			}
			line := fmt.Sprintf("    %s = %d", c.Name(), c.Value())
			if t, ok := c.Tags()["VersionMismatchType"]; ok {
				line += fmt.Sprintf("  (VersionMismatchType=%s)", t)
			}
			lines = append(lines, line)
		}
		sort.Strings(lines)
		if len(lines) == 0 {
			lines = []string{"    (no panic or version mismatch counters yet)"}
		}
		fmt.Printf("\n>>> METRICS\n%s\n\n", strings.Join(lines, "\n"))
	}
}

func start(service workflowserviceclient.Interface) {
	registerDomain(service)
	run, err := client.NewClient(service, domain, &client.Options{}).StartWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:                              workflowID,
		TaskList:                        taskList,
		ExecutionStartToCloseTimeout:    30 * time.Minute,
		DecisionTaskStartToCloseTimeout: 10 * time.Second,
	}, workflowName)
	check(err)
	fmt.Println("Started workflow", run.ID, "run", run.RunID)
}

func send(service workflowserviceclient.Interface) {
	check(client.NewClient(service, domain, &client.Options{}).SignalWorkflow(context.Background(), workflowID, "", signalName, nil))
	fmt.Println("Signal sent")
}

func check(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

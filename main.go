package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

/**
Needs to:
check config.conf
grab json file of applications
create registry of applications
start applications i not already started
start healthchecks
update application statuses

on failed healthchecks, update/patch registry
attempt to restart failing applications (if in config)
*/

const defaultTick = 2 * time.Second

type daemonConfig struct {
	monitoring bool
	port       int
	interval   time.Duration
	metrics    bool
	restart    bool
	forward    string
	appFile    string
}

func (config *daemonConfig) loadConfig(args []string) error {
	flags := flag.NewFlagSet(args[0], flag.ExitOnError)
	flags.String("I", "", "./config.conf")

	var (
		monitoring = flags.Bool("monitoring", false, "Monitoring")
		port       = flags.Int("port", 200, "Port to expose")
		interval   = flags.Duration("Interval", defaultTick, "Interval for monitoring requests")
		metrics    = flags.Bool("metrics", false, "Collect metrics")
		restart    = flags.Bool("restart", false, "Restart on failure")
		forward    = flags.String("forward", "", "Forward UDP logs to url") // -forward=http://localhost:6000/logs
		appFile    = flags.String("appFile", "", "Application list file")
	)

	if err := flags.Parse(args[1:]); err != nil {
		return err
	}

	config.monitoring = *monitoring
	config.port = *port
	config.interval = *interval
	config.metrics = *metrics
	config.restart = *restart
	config.forward = *forward
	config.appFile = *appFile

	log.Println("Config")
	fmt.Printf("%+v\n", config)

	// TODO(moosch): Create new log.Logger for each application.

	return nil
}

type serviceName string

type application struct {
	ServiceName  serviceName // "name": "NodeAPI",
	ServiceURL   string      // "url": "http://localhost",
	HeartbeatURL string      // "healthcheckURL": "/healthcheck",
	Runtime      string      // "runtime": "node",
	AppPath      string      // "path": "./node-app.js",
	Args         string      // "args": "--NODE_ENV=production",
	Port         int         // "port": 8080
}

type registry struct {
	applications []application
	mutex        *sync.RWMutex
}

func (r *registry) loadApplications(filepath string) error {
	content, err := ioutil.ReadFile(filepath)
	if err != nil {
		log.Printf("Failed to load app list from %v.", filepath)
		return err
	}

	var applications []application
	err = json.Unmarshal(content, &applications)
	if err != nil {
		log.Printf("Invalid app list from %v.", filepath)
		return err
	}

	r.applications = applications
	log.Println("Applications")
	fmt.Printf("%+v\n", applications)
	return nil
}

func (r *registry) add(reg application) {
	r.mutex.Lock()
	r.applications = append(r.applications, reg)
	r.mutex.Unlock()
}

func (r *registry) remove(url string) error {
	for i := range r.applications {
		if r.applications[i].ServiceURL == url {
			r.mutex.Lock()
			r.applications = append(r.applications[:i], r.applications[i+1:]...)
			r.mutex.Unlock()
			return nil
		}
	}
	return fmt.Errorf("Service at url %v not found", url)
}

func (r *registry) setupHealthchecks(freq time.Duration) {
	log.Printf("Setting up healthchecks for %d services\n", len(r.applications))
	for {
		var wg sync.WaitGroup
		for _, app := range r.applications {
			wg.Add(1)
			go func(app application) {
				defer wg.Done()
				success := true
				for attempts := 0; attempts < 3; attempts++ {
					res, err := http.Get(app.HeartbeatURL)
					if err != nil {
						log.Println(err)
					} else if res.StatusCode == http.StatusOK {
						log.Printf("%v is up.", app.ServiceName)
						// If previously failed, re-add to applications list
						if !success {
							r.add(app)
						}
						break
					}
					// Handle bad http response
					log.Printf("%v is down.", app.ServiceName)
					if success {
						success = false
						r.remove(string(app.ServiceURL))
					}
					// TODO(moosch): This could be more elegant. Progressive backoff or something to allow more time for reconnection.
					time.Sleep(1 * time.Second)
				}
			}(app)
			wg.Wait()
			time.Sleep(freq)
		}
	}
}

func main() {
	log.SetOutput(os.Stdout)
	log.Println("Starting Daemon.")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	signalChan := make(chan os.Signal, 1)
	// Relay process signals to signalChan
	signal.Notify(signalChan, os.Interrupt, syscall.SIGHUP)

	config := &daemonConfig{}

	registrations := registry{
		applications: make([]application, 0),
		mutex:        new(sync.RWMutex),
	}

	defer func() {
		signal.Stop(signalChan)
		cancel()
	}()

	go func() {
		for {
			select {
			case s := <-signalChan:
				switch s {
				case syscall.SIGHUP:
					config.loadConfig(os.Args)
				case os.Interrupt:
					cancel()
					os.Exit(1)
				}
			case <-ctx.Done():
				log.Println("Daemon shutting down.")
				os.Exit(1)
			}
		}
	}()

	if err := config.loadConfig(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Config error: %s\n", err)
		os.Exit(1)
	}

	if err := registrations.loadApplications(config.appFile); err != nil {
		fmt.Fprintf(os.Stderr, "Application loading error: %s\n", err)
		os.Exit(1)
	}

	if err := run(ctx, config); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	if err := startLogServer(config); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	registrations.setupHealthchecks(config.interval)
}

func run(ctx context.Context, config *daemonConfig) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.Tick(config.interval):
			// TODO(moosch): Loop through appplications and use go routines to check apps
			log.Println("Do healthchecks.")
		}
	}
}

/** Logging/Telemetry Server */

func startLogServer(config *daemonConfig) error {
	log.Println("Starting UDP log service.")
	port := strconv.Itoa(config.port)
	conn, err := net.ListenPacket("udp", ":"+port)
	if err != nil {
		log.Fatal("Failed to start log service.")
		return err
	}

	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		// NOTE(moosch): With the addr, we can track the "chatty" applications.
		_, addr, err := conn.ReadFrom(buf)
		if err != nil {
			continue
		}
		go forwardLog(conn, addr, buf, config.forward)
	}
}

func forwardLog(conn net.PacketConn, addr net.Addr, buf []byte, forwardURL string) {
	// 0 - 1: ID
	// 2: QR(1): Opcode(4)
	// buf[2] |= 0x80 // Set QR bit
	log.Printf("Log received: %v", buf)

	time := time.Now().Format(time.ANSIC)
	responseStr := fmt.Sprintf("time received: %v. Your message: %v!", time, string(buf))

	conn.WriteTo([]byte(responseStr), addr)

	// TODO(moosch): Forward on to URL
	// if forwardURL != "" {

	// }
}

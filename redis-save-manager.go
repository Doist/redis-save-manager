// Command redis-save-manager disables automatic persistence on a set of redis
// hosts and then runs BGSAVE on them sequentially in a loop, waiting for save
// to complete so that only one redis instance is saving data at a time.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/artyom/autoflags"
	"github.com/artyom/logger"
	"github.com/mediocregopher/radix.v2/redis"
)

func main() {
	args := struct {
		Deadline  time.Duration `flag:"deadline,exit after working for this long"`
		Addresses string        `flag:"file,file with redis addresses (host:port), one per line"`
	}{
		Deadline: time.Hour,
	}
	autoflags.Parse(&args)
	if args.Deadline < time.Minute {
		args.Deadline = time.Minute
	}
	log := log.New(os.Stderr, "", log.LstdFlags)
	addrs, err := readLines(args.Addresses)
	if err != nil {
		log.Fatal(err)
	}
	rand.Seed(time.Now().Unix())
	for i := range addrs {
		j := rand.Intn(i + 1)
		addrs[i], addrs[j] = addrs[j], addrs[i]
	}
	ctx, cancel := context.WithTimeout(context.Background(), args.Deadline)
	defer cancel()
	if err := do(ctx, log, addrs); err != nil {
		if err == context.DeadlineExceeded {
			log.Print("deadline of %v reached", args.Deadline)
			return
		}
		log.Fatal(err)
	}
}

func do(ctx context.Context, log logger.Interface, addrs []string) error {
	if len(addrs) == 0 {
		return errors.New("empty addresses")
	}
	for _, addr := range addrs {
		if err := disablePersistence(addr); err != nil {
			log.Printf("%s: %v", addr, err)
		}
	}
	for {
		for _, addr := range addrs {
			begin := time.Now()
			if err := saveBlocking(ctx, addr); err != nil {
				log.Printf("%s: %v", addr, err)
			} else {
				log.Printf("%s: saved in %v", addr, time.Since(begin))
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}
}

func saveBlocking(ctx context.Context, addr string) error {
	conn, err := redis.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	prev, err := conn.Cmd("LASTSAVE").Int64()
	if err != nil {
		return err
	}
	if err := conn.Cmd("BGSAVE").Err; err != nil {
		return err
	}
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			cur, err := conn.Cmd("LASTSAVE").Int64()
			if err != nil {
				return err
			}
			if cur != prev {
				return nil
			}
		}
	}
}

func disablePersistence(addr string) error {
	conn, err := redis.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Cmd("CONFIG", "SET", "SAVE", "").Err
}

func readLines(name string) ([]string, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var out []string
	for scanner.Scan() {
		out = append(out, string(bytes.TrimSpace(scanner.Bytes())))
	}
	return out, scanner.Err()
}

package keymux

import (
	"errors"
	"hash/crc32"
	"sync"
)

var (
	ErrStopped    = errors.New("mux is stopped")
	ErrFullBuffer = errors.New("worker buffer is full")
)

type panicError struct{ reason interface{} }

func (err panicError) Error() string { return "panic occurrs" }

type Handler interface {
	Handle(key []byte, val interface{}) (interface{}, error)
}

type job struct {
	key []byte
	val interface{}
	res chan res
}

type res struct {
	val interface{}
	err error
}

func (j job) do(handler Handler) {
	defer close(j.res)
	defer func() {
		if reason := recover(); reason != nil {
			j.res <- res{nil, panicError{reason}}
		}
	}()
	val, err := handler.Handle(j.key, j.val)
	j.res <- res{val, err}
}

type Mux struct {
	mu sync.RWMutex
	wg sync.WaitGroup
	ws []chan job
}

func Start(handler Handler, workers, bufferSize int) *Mux {
	m := new(Mux)
	m.Start(handler, workers, bufferSize)
	return m
}

func (m *Mux) Start(handler Handler, workers, bufferSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ws != nil {
		panic("mux is already started")
	}
	if workers <= 0 {
		panic("number of workers must be positive")
	}
	if bufferSize < 0 {
		panic("worker buffer size must not be negative")
	}
	m.wg.Add(workers)
	m.ws = make([]chan job, workers)
	for i := range m.ws {
		m.ws[i] = make(chan job, bufferSize)
		go func(jobs <-chan job) {
			defer m.wg.Done()
			for j := range jobs {
				j.do(handler)
			}
		}(m.ws[i])
	}
}

func (m *Mux) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, jobs := range m.ws {
		close(jobs)
	}
	m.ws = nil
	m.wg.Wait()
}

func (m *Mux) handle(crc uint32, j job) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.ws == nil {
		return ErrStopped
	}
	w := crc % uint32(len(m.ws))
	select {
	case m.ws[w] <- j:
		return nil
	default:
		return ErrFullBuffer
	}
}

func (m *Mux) Handle(key []byte, val interface{}) (interface{}, error) {
	j := job{key, val, make(chan res)}
	err := m.handle(crc32.ChecksumIEEE(key), j)
	if err != nil {
		return nil, err
	}
	r := <-j.res
	if p, ok := r.err.(panicError); ok {
		panic(p.reason)
	}
	return r.val, r.err
}

package keymux_test

import (
	"errors"
	"github.com/chiffa-org/keymux"
	"sync"
	"testing"
	"time"
)

type RealHandler struct {
	Delay time.Duration
}

type Op byte

const (
	Add Op = iota
	Sub Op = iota
	Mul Op = iota
	Div Op = iota
)

type Calc struct {
	Op   Op
	X, Y float64
}

var ErrUnknownValueType = errors.New("unknown value type")

func (h *RealHandler) Handle(key []byte, val interface{}) (interface{}, error) {
	if calc, ok := val.(Calc); ok {
		time.Sleep(h.Delay)
		switch calc.Op {
		case Add:
			return calc.X + calc.Y, nil
		case Sub:
			return calc.X - calc.Y, nil
		case Mul:
			return calc.X * calc.Y, nil
		case Div:
			return calc.X / calc.Y, nil // DANGER!!!
		}
	}
	return nil, ErrUnknownValueType
}

var ( // known to be serial on 4 workers
	Add3Key = []byte("add add add")
	Sub3Key = []byte("sub sub sub")
	Mul3Key = []byte("mul mul mul")
	Div3Key = []byte("div div div")
)

var ( // known to be parallel on 4 workers
	Add9Key = []byte("add add add add add add add add add")
	Sub9Key = []byte("sub sub sub sub sub sub sub sub sub")
	Mul9Key = []byte("mul mul mul mul mul mul mul mul mul")
	Div9Key = []byte("div div div div div div div div div")
)

func job(t *testing.T, mux *keymux.Mux, wg *sync.WaitGroup, key []byte, calc Calc, res float64) {
	obj, err := mux.Handle(key, calc)
	if err != nil {
		t.Error(err)
	}
	if f, ok := obj.(float64); !ok || f != res {
		t.Error("wrong result")
	}
	wg.Done()
}

func Test(t *testing.T) {
	handler := &RealHandler{10 * time.Millisecond}
	mux := keymux.Start(handler, 4, 4)
	defer mux.Stop()
	wg := new(sync.WaitGroup)
	wg.Add(4)
	t1 := time.Now()
	go job(t, mux, wg, Add9Key, Calc{Add, 3, 2}, 5)
	go job(t, mux, wg, Sub9Key, Calc{Sub, 3, 2}, 1)
	go job(t, mux, wg, Mul9Key, Calc{Mul, 3, 2}, 6)
	go job(t, mux, wg, Div9Key, Calc{Div, 3, 2}, 1.5)
	wg.Wait()
	t2 := time.Now()
	if t2.Sub(t1) >= 20*time.Millisecond {
		t.Error("too long")
	}
	if t2.Sub(t1) < 10*time.Millisecond {
		t.Error("too fast")
	}
}

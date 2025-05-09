package util

import (
	"runtime/debug"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/golang/glog"
)

func NonEmptyStrList(input []string) []string {
	output := make([]string, 0, len(input))
	for _, val := range input {
		if val == "" {
			glog.Warningf("empty item found in list")
			continue
		}
		output = append(output, val)
	}
	return output
}

func FromSliceOfInterface[T any](in []interface{}) []T {
	out := make([]T, 0, len(in))
	for _, v := range in {
		t := v.(T)
		out = append(out, t)
	}
	return out
}

func ToSliceOfInterface[T any](in []T) []interface{} {
	out := make([]interface{}, 0, len(in))
	for _, v := range in {
		out = append(out, v)
	}
	return out
}

func SetToSlice[T any](set mapset.Set) []T {
	return FromSliceOfInterface[T](set.ToSlice())
}

func SliceToSet[T any](slice []T) mapset.Set {
	return mapset.NewSetFromSlice(ToSliceOfInterface(slice))
}

/*
Search val in the slice: arr
if found, return the index
else return -1
*/
func Find[T comparable](arr []T, val T) int {
	for i, elem := range arr {
		if elem == val {
			return i
		}
	}
	return -1
}

func HandlePanicRecovery(f func()) {
	defer glog.Flush()
	defer RecoverPanic()
	f()
}

func RecoverPanic() {
	if r := recover(); r != nil {
		glog.Errorf("RECOVERED FROM PANIC:%v", r)
		glog.Errorf("STACKTRACE==>")
		glog.Errorf(string(debug.Stack()))
		glog.Errorf("<==STACKTRACE")
	}
}
func BatchExecute[T any](sl []T, batchSize int, executor func([]T) error) error {
	argSize := len(sl)
	glog.V(1).Infof("batchExecutor, argSize:%d", argSize)
	for i := 0; i < argSize; i += batchSize {
		start := i
		end := i + batchSize
		if end > argSize {
			end = argSize
		}
		err := executor(sl[start:end])
		if err != nil {
			return err
		}
	}
	return nil
}

func ExecuteQueryWithRetry[I any, O any](arg *I, f func(*I) (*O, error)) (*O, error) {
	var ret *O
	var err error
	if err := Retry(RetryConfig{Name: "idf_call", F: func() error {
		ret, err = f(arg)
		return err
	}, Delay: QueryExecutionBackoffDelay, MaxAttempts: QueryExecutionMaxAttempts}); err != nil {
		return nil, err
	}
	return ret, err
}

type RetryConfig struct {
	Name        string
	F           func() error
	Delay       time.Duration
	ExpFactor   time.Duration
	MaxAttempts int
	MaxDelay    time.Duration
}

func Retry(config RetryConfig) error {
	if config.ExpFactor <= 0 {
		config.ExpFactor = 1
	}
	delay := config.Delay

	i := 0
	for {
		err := config.F()
		i++
		if err == nil {
			if i != 1 {
				glog.Infof("Retry attempt succeeded:[function:%v, attempts:%v]", config.Name, i-1)
			}
			return nil
		}
		if config.MaxAttempts > 0 && i == config.MaxAttempts+1 {
			glog.Errorf("Retry attempt exhausted:[function:%v, attempts:%v]", config.Name, config.MaxAttempts)
			return err
		}
		glog.Errorf("Retry attempt:[for:%v, after:%v attempt:%v, reason:[%v]]", config.Name, delay, i, err)
		time.Sleep(delay)
		delay *= config.ExpFactor
		if config.MaxDelay > 0 && delay > config.MaxDelay {
			delay = config.MaxDelay
		}
	}
}

// Batcher
/**
A generic batching function.
It accepts a streaming input, collects a batch and processes it. This repeats till the end of the stream.
The input is specified as a channel iterator, from which the input entities of type I are pulled.
The pulled entities are accumulated till the batchSize is reached.
The batch is serially fed into the caller-provided processor function, to be processed.
This repeats till there are no more elements in the stream. To achieve this, the channel must be closed by the caller.
*/
func Batcher[InputType any](operation string, batchSize int, total int, iterator func(chan InputType),
	processor func([]InputType) error) error {
	t0 := time.Now()
	totalBatches := total / batchSize
	if total%batchSize > 0 {
		totalBatches++
	}
	glog.Infof("performing operation[%v] containing %v items in %v batches with batchSize of %v", operation, total, totalBatches, batchSize)
	currentBatch := 1
	itemsArr := make([]InputType, 0, batchSize)
	itemsSlice := itemsArr[0:0]
	ch := make(chan InputType)
	go iterator(ch)
	for arg := range ch {
		itemsSlice = append(itemsSlice, arg)
		if len(itemsSlice) == cap(itemsArr) {
			glog.Infof("batch %v for [%v] in progress..", currentBatch, operation)
			t1 := time.Now()
			err := processor(itemsSlice)
			if err != nil {
				glog.Errorf("error processing batch %v for [%v]:%v", currentBatch, operation, err)
				return err
			}
			glog.Infof("batch %v completed in %v ms", currentBatch, time.Since(t1).Milliseconds())
			itemsSlice = itemsArr[0:0]
			currentBatch++
		}
	}
	if len(itemsSlice) > 0 {
		glog.Infof("batch %v for [%v] in progress..", currentBatch, operation)
		t1 := time.Now()
		err := processor(itemsSlice)
		if err != nil {
			glog.Errorf("error processing batch %v for [%v]:%v", currentBatch, operation, err)
			return err
		}
		glog.Infof("batch %v completed in %v ms", currentBatch, time.Since(t1).Milliseconds())
	}
	glog.Infof("batched operation [%v] completed in %v ms", operation, time.Since(t0).Milliseconds())
	return nil
}

// IsEmpty checks if the given string is empty.
func IsEmpty(key string) bool {
	return len(key) == 0
}

func GetFirstItemString(items []string) string {
	if items == nil || len(items) == 0 {
		return EmptyString
	}
	return items[0]
}

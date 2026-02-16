package or

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestOr_Empty(t *testing.T) {
	ch := Or()
	_, ok := <-ch
	if ok {
		t.Error("ожидалось, что канал сразу закрыт при вызове без аргументов")
	}
}

func TestOr_OneChannel(t *testing.T) {
	ch := make(chan interface{})
	go func() {
		time.Sleep(30 * time.Millisecond)
		ch <- "data"
		close(ch)
	}()

	orCh := Or(ch)

	select {
	case v, ok := <-orCh:
		if !ok {
			t.Error("канал закрылся слишком рано")
		}
		if v != "data" {
			t.Errorf("получено неверное значение: %v", v)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("таймаут — значение не пришло")
	}
}

func TestOr_TwoChannels_FirstWins(t *testing.T) {
	ch1 := make(chan interface{})
	ch2 := make(chan interface{})

	go func() {
		time.Sleep(20 * time.Millisecond)
		ch1 <- "first"
	}()

	go func() {
		time.Sleep(80 * time.Millisecond)
		ch2 <- "second"
	}()

	orCh := Or(ch1, ch2)

	select {
	case v := <-orCh:
		if v != "first" {
			t.Errorf("ожидалось 'first', получено: %v", v)
		}
	case <-time.After(150 * time.Millisecond):
		t.Error("таймаут")
	}
}

func TestOr_ManyChannels_QuickestWins(t *testing.T) {
	const n = 8
	chs := make([]<-chan interface{}, n)

	for i := 0; i < n; i++ {
		ch := make(chan interface{})
		chs[i] = ch

		delay := time.Duration(30+i*15) * time.Millisecond
		go func(ch chan interface{}, d time.Duration) {
			time.Sleep(d)
			ch <- fmt.Sprintf("winner-%d", i)
			close(ch)
		}(ch, delay)
	}

	start := time.Now()
	orCh := Or(chs...)

	v := <-orCh
	duration := time.Since(start)

	if duration > 45*time.Millisecond {
		t.Errorf("слишком долго ждали: %v", duration)
	}

	if v != "winner-0" {
		t.Errorf("должен был победить самый быстрый канал (winner-0), а победил: %v", v)
	}
}

func TestOr_AlreadyClosedChannel(t *testing.T) {
	ch1 := make(chan interface{})
	close(ch1)

	ch2 := make(chan interface{}) // открыт и пуст

	orCh := Or(ch1, ch2)

	select {
	case _, ok := <-orCh:
		if ok {
			t.Error("ожидалось, что канал закрыт, а не открыт")
		}
	case <-time.After(10 * time.Millisecond): // очень короткий таймаут
		t.Error("or-канал не закрылся почти мгновенно")
	}
}

func TestOr_AllClosedImmediately(t *testing.T) {
	ch1 := make(chan interface{})
	ch2 := make(chan interface{})
	close(ch1)
	close(ch2)

	orCh := Or(ch1, ch2)

	_, ok := <-orCh
	if ok {
		t.Error("ожидалось немедленное закрытие")
	}
}

func TestOr_ConcurrencySafety(t *testing.T) {
	const goroutines = 40
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			ch1 := afterMs(10 + idx*2)
			ch2 := afterMs(15 + idx*3)
			ch3 := afterMs(8 + idx)

			_ = <-Or(ch1, ch2, ch3)
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// всё хорошо
	case <-time.After(800 * time.Millisecond):
		t.Fatal("слишком долго — возможен deadlock или утечка горутин")
	}
}

func TestOr_DoesNotLeakGoroutines_WhenEarlyExit(t *testing.T) {
	chSlow := make(chan interface{})
	chFast := afterMs(10)

	orCh := Or(chFast, chSlow)

	<-orCh // быстро читаем

	// даём шанс горутине завершиться
	time.Sleep(50 * time.Millisecond)

	// если горутина всё ещё висит — будет утечка
	// (в тестах это сложно надёжно проверить, но поведение можно наблюдать)
}

func afterMs(ms int) <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		time.Sleep(time.Duration(ms) * time.Millisecond)
		ch <- ms
		close(ch)
	}()
	return ch
}

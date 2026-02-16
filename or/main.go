package or

// Необходимо реализовать функцию объединения done-каналов (описана в задаче 14 уровня 2).
// Проект хоть и небольшой, но концептуально важен: правильная работа с конкурентностью и каналами.
//
// Готовая функция может быть оформлена как утилита/пакет с примерами использования и тестами.
//
// Результат: пакет or (например, or.go + or_test.go), экспортирующий функцию Or(ch1, ch2, ... chN <-chan interface{}) <-chan interface{}.
//
// Дополнительно: реализовать тесты (or_test.go) и пример использования.
func isClosed(ch <-chan interface{}) bool {
	select {
	case _, ok := <-ch:
		return !ok
	default:
		return false
	}
}

func Or(chs ...<-chan interface{}) <-chan interface{} {
	if len(chs) == 0 {
		done := make(chan interface{})
		close(done)
		return done
	}
	if len(chs) == 1 {
		return chs[0]
	}

	// Если хотя бы один уже закрыт — сразу возвращаем закрытый канал
	for _, ch := range chs {
		if isClosed(ch) {
			done := make(chan interface{})
			close(done)
			return done
		}
	}

	mid := len(chs) / 2
	return or2(Or(chs[:mid]...), Or(chs[mid:]...))
}

func or2(a, b <-chan interface{}) <-chan interface{} {
	done := make(chan interface{})
	go func() {
		defer close(done)
		select {
		case v, ok := <-a:
			if ok {
				done <- v
			}
		case v, ok := <-b:
			if ok {
				done <- v
			}
		}
	}()
	return done
}

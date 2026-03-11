package runner

import (
	"sync"
	"testing"
)

func TestSubscriptionRouter_Match_ExactSelector(t *testing.T) {
	router := newSubscriptionRouter()

	subs := map[string]string{
		"plugin.device.entity.action": "Handler1",
	}
	selectors := []string{
		"plugin.device.entity.action",
		"plugin.device.entity.other",
	}

	triggered := router.match(subs, selectors)

	if len(triggered) != 1 {
		t.Errorf("expected 1 triggered handler, got %d", len(triggered))
	}
	if _, ok := triggered["Handler1"]; !ok {
		t.Error("expected Handler1 to be triggered")
	}
}

func TestSubscriptionRouter_Match_WildcardPattern(t *testing.T) {
	router := newSubscriptionRouter()

	subs := map[string]string{
		"plugin.*.entity.action": "Handler1",
	}
	selectors := []string{
		"plugin.device1.entity.action",
		"plugin.device2.entity.action",
		"plugin.device1.entity.other",
	}

	triggered := router.match(subs, selectors)

	if len(triggered) != 1 {
		t.Errorf("expected 1 triggered handler, got %d", len(triggered))
	}
	if _, ok := triggered["Handler1"]; !ok {
		t.Error("expected Handler1 to be triggered")
	}
}

func TestSubscriptionRouter_Match_Deduplication(t *testing.T) {
	router := newSubscriptionRouter()

	// Multiple selectors match the same handler
	subs := map[string]string{
		"plugin.*.entity.*": "Handler1",
	}
	selectors := []string{
		"plugin.device1.entity.action1",
		"plugin.device1.entity.action2",
	}

	triggered := router.match(subs, selectors)

	// Handler should only appear once
	if len(triggered) != 1 {
		t.Errorf("expected 1 triggered handler (deduplicated), got %d", len(triggered))
	}
}

func TestSubscriptionRouter_MatchPattern_ExactMatch(t *testing.T) {
	router := newSubscriptionRouter()

	// Same pattern and actual
	if !router.matchPattern("plugin.device.entity", "plugin.device.entity") {
		t.Error("exact match should return true")
	}

	// Different strings
	if router.matchPattern("plugin.device.entity", "other.device.entity") {
		t.Error("different strings should not match")
	}
}

func TestSubscriptionRouter_MatchPattern_Wildcards(t *testing.T) {
	router := newSubscriptionRouter()

	tests := []struct {
		pattern string
		actual  string
		want    bool
	}{
		{"*", "anything", true},
		{"plugin.*.entity", "plugin.device.entity", true},
		{"plugin.*.entity", "plugin.other.entity", true},
		{"plugin.*.entity", "plugin.device.other", false},
		{"plugin.device.*", "plugin.device.entity", true},
		{"*.device.*", "plugin.device.entity", true},
		{"plugin.*.*", "plugin.device.entity", true},
	}

	for _, tt := range tests {
		got := router.matchPattern(tt.pattern, tt.actual)
		if got != tt.want {
			t.Errorf("matchPattern(%q, %q) = %v, want %v", tt.pattern, tt.actual, got, tt.want)
		}
	}
}

func TestSubscriptionRouter_MatchPattern_CacheHit(t *testing.T) {
	router := newSubscriptionRouter()

	pattern := "plugin.*.entity"
	actual := "plugin.device.entity"

	// First call should compile and cache
	router.matchPattern(pattern, actual)

	// Check cache
	router.mu.RLock()
	_, cached := router.cache[pattern]
	router.mu.RUnlock()

	if !cached {
		t.Error("pattern should be cached")
	}

	// Second call should use cache
	if !router.matchPattern(pattern, actual) {
		t.Error("second call should also match")
	}
}

func TestSubscriptionRouter_MatchPattern_DoubleStar(t *testing.T) {
	router := newSubscriptionRouter()

	tests := []struct {
		pattern string
		actual  string
		want    bool
	}{
		{"**", "anything.at.all", true},
		{"**", "single", true},
		{"plugin.**.entity", "plugin.a.b.c.entity", true},
		{"plugin.**.entity", "plugin.device.entity", true},
		{"plugin.**.entity", "plugin.entity", false}, // needs at least one segment between
	}

	for _, tt := range tests {
		got := router.matchPattern(tt.pattern, tt.actual)
		if got != tt.want {
			t.Errorf("matchPattern(%q, %q) = %v, want %v", tt.pattern, tt.actual, got, tt.want)
		}
	}
}

func TestSubscriptionRouter_ConcurrentMatch_Safe(t *testing.T) {
	router := newSubscriptionRouter()

	subs := map[string]string{
		"plugin.*.entity.*": "Handler1",
		"*.device.*.action": "Handler2",
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			selectors := []string{
				"plugin.device" + string(rune('0'+i%10)) + ".entity.action",
			}
			router.match(subs, selectors)
		}(i)
	}
	wg.Wait()

	// Cache should have entries
	router.mu.RLock()
	cacheSize := len(router.cache)
	router.mu.RUnlock()

	if cacheSize == 0 {
		t.Error("cache should have compiled patterns")
	}
}

func TestSubscriptionRouter_InvalidPattern(t *testing.T) {
	router := newSubscriptionRouter()

	// Invalid regex pattern should not panic
	if router.matchPattern("[invalid", "test") {
		t.Error("invalid pattern should return false")
	}

	// Cache should not have the invalid pattern
	router.mu.RLock()
	_, cached := router.cache["[invalid"]
	router.mu.RUnlock()

	if cached {
		t.Error("invalid pattern should not be cached")
	}
}

func TestSubscriptionRouter_MultipleHandlers(t *testing.T) {
	router := newSubscriptionRouter()

	subs := map[string]string{
		"plugin.device1.entity.*": "Handler1",
		"plugin.device2.entity.*": "Handler2",
		"plugin.*.entity.action":  "Handler3",
	}
	selectors := []string{
		"plugin.device1.entity.action",
		"plugin.device2.entity.action",
	}

	triggered := router.match(subs, selectors)

	// All three handlers should be triggered
	// Handler1 matches device1.action, Handler2 matches device2.action
	// Handler3 matches both via wildcard
	if len(triggered) != 3 {
		t.Errorf("expected 3 triggered handlers, got %d: %v", len(triggered), triggered)
	}

	for _, handler := range []string{"Handler1", "Handler2", "Handler3"} {
		if _, ok := triggered[handler]; !ok {
			t.Errorf("expected %s to be triggered", handler)
		}
	}
}

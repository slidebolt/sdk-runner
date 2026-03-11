package runner

import (
	"regexp"
	"strings"
	"sync"
)

// subscriptionRouter matches event/command selectors against registered subscriptions.
// It owns the compiled-regex cache for wildcard patterns.
type subscriptionRouter struct {
	mu    sync.RWMutex
	cache map[string]*regexp.Regexp
}

func newSubscriptionRouter() *subscriptionRouter {
	return &subscriptionRouter{cache: make(map[string]*regexp.Regexp)}
}

// match returns a map of {handlerName → matchedSelector} for all subscriptions
// that match at least one of the provided selectors. Each handler is triggered
// at most once (deduplication by handler name).
func (r *subscriptionRouter) match(subs map[string]string, selectors []string) map[string]string {
	triggered := make(map[string]string)

	// Fast path: exact lookup for each generated selector.
	for _, sel := range selectors {
		if handler, ok := subs[sel]; ok && handler != "" {
			triggered[handler] = sel
		}
	}

	// Slow path: wildcard pattern matching.
	for subSel, handler := range subs {
		if _, done := triggered[handler]; done {
			continue
		}
		if !strings.Contains(subSel, "*") {
			continue
		}
		for _, sel := range selectors {
			if r.matchPattern(subSel, sel) {
				triggered[handler] = subSel
				break
			}
		}
	}
	return triggered
}

func (r *subscriptionRouter) matchPattern(pattern, actual string) bool {
	if pattern == actual || pattern == "*" || pattern == "**" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}

	r.mu.RLock()
	re, ok := r.cache[pattern]
	r.mu.RUnlock()

	if !ok {
		regPattern := "^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), "\\*\\*", ".*")
		regPattern = strings.ReplaceAll(regPattern, "\\*", "[^.]+")
		regPattern += "$"
		var err error
		re, err = regexp.Compile(regPattern)
		if err != nil {
			return false
		}
		r.mu.Lock()
		r.cache[pattern] = re
		r.mu.Unlock()
	}
	return re.MatchString(actual)
}

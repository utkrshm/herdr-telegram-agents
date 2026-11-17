package telegram

import (
	"html"
	"strconv"
	"strings"
	"unicode/utf16"
)

const (
	// textMax stays below Telegram's 4096-character limit with room for the
	// multi-part suffix and formatting wrappers.
	textMax = 4000
	// defaultTopicName is used when a label is empty after trimming.
	defaultTopicName = "agent"
)

// chunk splits text into pieces of at most max UTF-16 code units, which is
// how Telegram measures message length (an emoji outside the BMP costs
// two). It prefers to cut at the last newline in the second half of a
// piece so lines stay whole; a newline that becomes a cut point is dropped
// rather than carried over. Runes are never split.
func chunk(text string, max int) []string {
	if text == "" {
		return nil
	}
	if max <= 0 {
		return []string{text}
	}
	var out []string
	rest := []rune(text)
	for {
		n := fitUTF16(rest, max)
		if n == len(rest) {
			return append(out, string(rest))
		}
		cut := n
		if nl := lastIndexRune(rest[:n], '\n'); nl > n/2 {
			cut = nl
		}
		out = append(out, string(rest[:cut]))
		rest = rest[cut:]
		if len(rest) > 0 && rest[0] == '\n' {
			rest = rest[1:]
		}
		if len(rest) == 0 {
			return out
		}
	}
}

// splitResponse splits a response without dropping or rewriting source
// characters. Sentence and newline boundaries are preferred; oversized
// sentences fall back to rune-safe UTF-16 boundaries. Every part carries a
// chunk-number/total suffix, whose varying width is accounted for iteratively.
type responsePart struct{ content, suffix string }

func splitResponse(text string, max int) []responsePart {
	if text == "" {
		return nil
	}
	if utf16Len(text) <= max {
		return []responsePart{{content: text}}
	}
	total := 2
	for attempt := 0; attempt < 16; attempt++ {
		parts := splitResponseForTotal(text, max, total)
		if len(parts) == total {
			return parts
		}
		if len(parts) < 2 {
			return []responsePart{{content: text}}
		}
		total = len(parts)
	}
	return splitResponseForTotal(text, max, total+1)
}

func splitResponseForTotal(text string, max, total int) []responsePart {
	var parts []responsePart
	for len(text) > 0 {
		index := len(parts)
		suffix := responseSuffix(index+1, total)
		budget := max - utf16Len(suffix)
		if utf16Len(text) <= budget {
			parts = append(parts, responsePart{text, suffix})
			break
		}
		rs := []rune(text)
		fit := fitUTF16(rs, budget)
		cut := naturalCut(rs[:fit])
		if cut <= 0 {
			cut = fit
		}
		if cut <= 0 {
			cut = 1
		}
		parts = append(parts, responsePart{string(rs[:cut]), suffix})
		text = string(rs[cut:])
	}
	return parts
}

func responseSuffix(chunk, total int) string {
	return "\n… (" + strconv.Itoa(chunk) + "/" + strconv.Itoa(total) + ")"
}

func naturalCut(rs []rune) int {
	min := len(rs) / 2
	for i := len(rs) - 1; i >= min; i-- {
		switch rs[i] {
		case '\n', '.', '!', '?', '。', '！', '？':
			return i + 1
		}
	}
	return len(rs)
}

// fitUTF16 returns how many leading runes of rs fit into max UTF-16 code
// units, always at least one so a caller can make progress.
func fitUTF16(rs []rune, max int) int {
	units := 0
	for i, r := range rs {
		u := utf16.RuneLen(r)
		if u < 0 {
			u = 1
		}
		if units+u > max && i > 0 {
			return i
		}
		units += u
	}
	return len(rs)
}

func lastIndexRune(rs []rune, r rune) int {
	for i := len(rs) - 1; i >= 0; i-- {
		if rs[i] == r {
			return i
		}
	}
	return -1
}

// renderPlain escapes a chunk for parse_mode HTML.
func renderPlain(part string) string {
	return html.EscapeString(part)
}

// renderCode wraps an escaped chunk in <pre> for terminal output.
func renderCode(part string) string {
	return "<pre>" + html.EscapeString(part) + "</pre>"
}

// truncateName trims a topic name to max UTF-16 code units (Telegram's
// unit for the 128-character topic limit) and substitutes defaultTopicName
// for an empty one.
func truncateName(s string, max int) string {
	s = strings.TrimSpace(s)
	if s == "" {
		s = defaultTopicName
	}
	r := []rune(s)
	if n := fitUTF16(r, max); n < len(r) {
		return string(r[:n])
	}
	return s
}

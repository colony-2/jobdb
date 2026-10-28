package jobdb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

type JSONPayloadStats struct {
	RootMembers        []JSONPayloadField `json:"rootMembers,omitempty"`
	RootRemainderBytes *int               `json:"rootRemainderBytes,omitempty"`
	RootElements       int                `json:"rootElements,omitempty"`
	Status             string             `json:"status"`
	RootKind           string             `json:"rootKind,omitempty"`
	Members            int                `json:"observedObjectMembers"`
	Elements           int                `json:"observedArrayElements"`
	Scalars            int                `json:"observedScalars"`
	MaxDepth           int                `json:"observedMaxDepth"`
	LargestStringBytes int                `json:"observedLargestStringBytes"`
	Largest            []JSONPayloadField `json:"largestInclusiveSubtrees,omitempty"`
}
type JSONPayloadField struct {
	Path     string `json:"path"`
	Kind     string `json:"kind"`
	Bytes    int    `json:"bytes"`
	Elements int    `json:"elements,omitempty"`
}

func boundedPath(s string) string {
	if len(s) <= 160 {
		return s
	}
	s = s[:157]
	for !utf8.ValidString(s) && len(s) > 0 {
		s = s[:len(s)-1]
	}
	return s + "..."
}

type payloadScanner struct {
	ctx     context.Context
	raw     []byte
	pos     int
	tokens  *int
	stats   *JSONPayloadStats
	top     int
	stopped bool
}

func analyzeJSONPayload(ctx context.Context, raw []byte, budget, tokens *int, top int) *JSONPayloadStats {
	s := &JSONPayloadStats{Status: "partial"}
	if ctx == nil {
		ctx = context.Background()
	}
	if *budget <= 0 || *tokens <= 0 || ctx.Err() != nil {
		return s
	}
	n := len(raw)
	if n > *budget {
		n = *budget
	}
	*budget -= n
	complete := n == len(raw)
	if complete && !json.Valid(raw) {
		s.Status = "non_json"
		return s
	}
	scanner := payloadScanner{ctx: ctx, raw: raw[:n], tokens: tokens, stats: s, top: top}
	kind, count := scanner.value("", 0)
	if kind == "array" {
		s.RootElements = count
	}
	s.RootKind = kind
	if complete && !scanner.stopped {
		s.Status = "complete"
		remainder := len(raw)
		for _, f := range s.RootMembers {
			remainder -= f.Bytes
		}
		s.RootRemainderBytes = &remainder
	}
	return s
}
func (p *payloadScanner) space() {
	for p.pos < len(p.raw) {
		switch p.raw[p.pos] {
		case ' ', '\n', '\t', '\r':
			p.pos++
		default:
			return
		}
	}
}
func (p *payloadScanner) quoted() (int, int, bool) {
	start := p.pos
	p.pos++
	for p.pos < len(p.raw) {
		c := p.raw[p.pos]
		p.pos++
		if c == '"' {
			return start, p.pos, true
		}
		if c == '\\' {
			p.pos++
		}
	}
	p.stopped = true
	return start, p.pos, false
}
func (p *payloadScanner) value(path string, depth int) (string, int) {
	if p.stopped {
		return "", 0
	}
	p.space()
	start := p.pos
	if p.pos >= len(p.raw) || depth > 64 || *p.tokens <= 0 || p.ctx.Err() != nil {
		p.stopped = true
		return "", 0
	}
	*p.tokens--
	if depth > p.stats.MaxDepth {
		p.stats.MaxDepth = depth
	}
	kind := "scalar"
	elements := 0
	switch p.raw[p.pos] {
	case '"':
		kind = "string"
		_, end, ok := p.quoted()
		if !ok {
			return kind, 0
		}
		p.stats.Scalars++
		if end-start > p.stats.LargestStringBytes {
			p.stats.LargestStringBytes = end - start
		}
	case '{', '[':
		object := p.raw[p.pos] == '{'
		kind = "array"
		end := byte(']')
		if object {
			kind = "object"
			end = '}'
		}
		if depth == 0 {
			p.stats.RootKind = kind
		}
		p.pos++
		p.space()
		occurrences := map[string]int{}
		for p.pos < len(p.raw) && p.raw[p.pos] != end {
			name := fmt.Sprintf("%d", elements)
			if object {
				if p.raw[p.pos] != '"' {
					p.stopped = true
					return kind, elements
				}
				a, b, ok := p.quoted()
				if !ok {
					return kind, elements
				}
				name = "<long-key>"
				if b-a <= 160 {
					_ = json.Unmarshal(p.raw[a:b], &name)
				}
				name = strings.ReplaceAll(strings.ReplaceAll(name, "~", "~0"), "/", "~1")
				// Bound the duplicate-key tracking map as well as the emitted report.
				if len(occurrences) < 1024 {
					occurrences[name]++
					if occurrences[name] > 1 {
						name += fmt.Sprintf("#occurrence:%d", occurrences[name])
					}
				}
				p.space()
				if p.pos >= len(p.raw) || p.raw[p.pos] != ':' {
					p.stopped = true
					return kind, elements
				}
				p.pos++
				p.stats.Members++
			} else {
				p.stats.Elements++
			}
			p.value(boundedPath(path+"/"+name), depth+1)
			if p.stopped {
				return kind, elements
			}
			elements++
			p.space()
			if p.pos < len(p.raw) && p.raw[p.pos] == ',' {
				p.pos++
				p.space()
			} else {
				break
			}
		}
		if p.pos >= len(p.raw) || p.raw[p.pos] != end {
			p.stopped = true
			return kind, elements
		}
		p.pos++
	default:
		switch p.raw[p.pos] {
		case 'n':
			kind = "null"
		case 't', 'f':
			kind = "boolean"
		default:
			kind = "number"
		}
		for p.pos < len(p.raw) && !strings.ContainsRune(",]} \t\n\r", rune(p.raw[p.pos])) {
			p.pos++
		}
		p.stats.Scalars++
	}
	if depth > 0 && p.top > 0 {
		f := JSONPayloadField{Path: boundedPath(path), Kind: kind, Bytes: p.pos - start}
		if kind == "array" {
			f.Elements = elements
		}
		if depth == 1 && p.stats.RootKind != "array" {
			p.stats.RootMembers = append(p.stats.RootMembers, f)
			sort.SliceStable(p.stats.RootMembers, func(i, j int) bool { return p.stats.RootMembers[i].Bytes > p.stats.RootMembers[j].Bytes })
			if len(p.stats.RootMembers) > p.top {
				p.stats.RootMembers = p.stats.RootMembers[:p.top]
			}
		}
		p.stats.Largest = append(p.stats.Largest, f)
		sort.SliceStable(p.stats.Largest, func(i, j int) bool { return p.stats.Largest[i].Bytes > p.stats.Largest[j].Bytes })
		if len(p.stats.Largest) > p.top {
			p.stats.Largest = p.stats.Largest[:p.top]
		}
	}
	return kind, elements
}

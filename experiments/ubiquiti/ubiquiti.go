package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gravwell/gravwell/v3/ingest"
	"github.com/gravwell/gravwell/v3/ingest/entry"
	"github.com/gravwell/gravwell/v3/ingest/log"
	"github.com/gravwell/gravwell/v3/ingest/processors"
	"github.com/gravwell/gravwell/v3/ingesters/base"
	"golang.org/x/time/rate"
)

// ------------------------------- constants -----------------------------------

const (
	unifiDefaultDomain   = "api.ui.com" // Site Manager base
	unifiDefaultAPIPath  = "/v1/hosts"  // safe default that always exists
	unifiDefaultTimeout  = 20 * time.Second
	unifiEmptySleepDur   = 15 * time.Second
	unifiDefaultRPM      = 100
	unifiDefaultBurst    = 100
	retryAfterHeaderName = "Retry-After"
)

// --------------------------- handler configuration ---------------------------

var ubiquitiConns map[string]*ubiquitiHandlerConfig

type ubiquitiHandlerConfig struct {
	// config
	Domain      string
	Token       string
	API         string
	Endpoints   []string // router/expand templates
	Mode        string   // "single", "cursor", "timewindow", "router", "expand"
	StartTime   time.Time
	Tag         entry.EntryTag
	Name        string
	Rate        int
	PreprocList []string

	// expand config
	ExpandToken string // default "{id}"
	ExpandIDKey string // default "id"

	// transport config
	Insecure bool // allow invalid TLS (UDM/CK often uses self-signed)

	// framework
	SRC  net.IP
	WG   *sync.WaitGroup
	PROC *processors.ProcessorSet
	CTX  context.Context
	OT   *objectTracker
}

// ------------------------------- builder -------------------------------------

func buildUbiquitiHandlerConfig(cfg *cfgType, src net.IP, ot *objectTracker, lg *log.Logger,
	igst *ingest.IngestMuxer, ib base.IngesterBase, ctx context.Context, wg *sync.WaitGroup) *ubiquitiHandlerConfig {

	_ = ib // reserved for future use

	ubiquitiConns = make(map[string]*ubiquitiHandlerConfig)

	for name, vc := range cfg.UbiquitiConf {
		tag, err := igst.GetTag(vc.Tag_Name)
		if err != nil {
			lg.Fatal("failed to resolve tag", log.KV("listener", name), log.KV("tag", vc.Tag_Name), log.KVErr(err))
		}

		// ensure state object
		if _, ok := ot.Get("ubiquiti", name); !ok {
			state := trackedObjectState{
				Updated:    time.Now(),
				LatestTime: vc.StartTime,
				Key:        "",
			}
			if err := ot.Set("ubiquiti", name, state, false); err != nil {
				lg.Fatal("failed to set state tracker", log.KV("listener", name), log.KVErr(err))
			}
			if err := ot.Flush(); err != nil {
				lg.Fatal("failed to flush state tracker", log.KV("listener", name), log.KVErr(err))
			}
		}

		if src == nil {
			src = net.ParseIP("127.0.0.1")
		}

		api := vc.API
		if api == "" && strings.ToLower(vc.Mode) != "router" {
			api = unifiDefaultAPIPath
		}
		domain := vc.Domain
		if domain == "" {
			domain = unifiDefaultDomain
		}
		rateLim := vc.RateLimit
		if rateLim <= 0 {
			rateLim = unifiDefaultRPM
		}

		expandToken := vc.ExpandToken
		if strings.TrimSpace(expandToken) == "" {
			expandToken = "{id}"
		}
		expandIDKey := vc.ExpandIDKey
		if strings.TrimSpace(expandIDKey) == "" {
			expandIDKey = "id"
		}

		h := &ubiquitiHandlerConfig{
			Domain:      domain,
			Token:       vc.Token,
			API:         api,
			Endpoints:   vc.Endpoints,
			Mode:        strings.ToLower(vc.Mode),
			StartTime:   vc.StartTime,
			Name:        name,
			Tag:         tag,
			SRC:         src,
			WG:          wg,
			CTX:         ctx,
			OT:          ot,
			Rate:        rateLim,
			PreprocList: vc.Preprocessor,
			Insecure:    vc.Insecure,

			ExpandToken: expandToken,
			ExpandIDKey: expandIDKey,
		}

		if h.PROC, err = cfg.Preprocessor.ProcessorSet(igst, vc.Preprocessor); err != nil {
			lg.Fatal("preprocessor construction error", log.KVErr(err))
		}

		ubiquitiConns[name] = h
	}

	for _, h := range ubiquitiConns {
		wg.Add(1)
		go h.run()
	}

	return nil
}

// --------------------------------- runtime -----------------------------------

func (h *ubiquitiHandlerConfig) run() {
	defer h.WG.Done()

	client := buildHTTPClient(h.Insecure)
	rl := rate.NewLimiter(rate.Every(time.Minute/time.Duration(h.Rate)), unifiDefaultBurst)

	for {
		select {
		case <-h.CTX.Done():
			return
		default:
			state, ok := h.OT.Get("ubiquiti", h.Name)
			if !ok {
				lg.Error("state tracker missing", log.KV("ubiquiti", h.Name))
				return
			}

			switch h.Mode {
			case "cursor":
				_ = h.fetchCursor(client, rl, state)
			case "timewindow":
				_ = h.fetchTimeWindow(client, rl, state)
			case "router":
				_ = h.fetchMultiEndpoint(client, rl, state)
			case "expand":
				_ = h.fetchExpand(client, rl, state)
			default: // "single"
				_ = h.fetchSingle(client, rl, state)
			}

			_ = h.OT.Flush()

			if quitableSleep(h.CTX, unifiEmptySleepDur) {
				return
			}
		}
	}
}

func buildHTTPClient(insecure bool) *http.Client {
	// clone default transport to avoid mutating global defaults
	var tr *http.Transport
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		tr = dt.Clone()
	} else {
		tr = &http.Transport{}
	}
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: insecure}

	return &http.Client{
		Timeout:   unifiDefaultTimeout,
		Transport: tr,
	}
}

// -------------------------- response / timestamp -----------------------------

type smEnvelope struct {
	Data           json.RawMessage `json:"data"`
	HTTPStatusCode int             `json:"httpStatusCode"`
	TraceID        string          `json:"traceId"`
}

func extractTimestamp(raw json.RawMessage) (time.Time, error) {
	// best-effort hook: use common fields if present; fallback to now
	type ts1 struct {
		Timestamp string `json:"timestamp"`
	}
	var t1 ts1
	if err := json.Unmarshal(raw, &t1); err == nil && t1.Timestamp != "" {
		if t, err := time.Parse(time.RFC3339, t1.Timestamp); err == nil {
			return t, nil
		}
	}
	return time.Now(), nil
}

// ------------------------------- fetch modes ---------------------------------

func (h *ubiquitiHandlerConfig) fetchSingle(client *http.Client, rl *rate.Limiter, state trackedObjectState) error {
	if err := rl.Wait(h.CTX); err != nil {
		return err
	}
	u := fmt.Sprintf("https://%s%s", h.Domain, ensureLeadingSlash(h.API))
	req, _ := http.NewRequest(http.MethodGet, u, nil)
	req.Header.Set("X-API-KEY", h.Token)
	req.Header.Set("Accept", "application/json")

	resp, err := doRequest(client, req, h.CTX)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return h.emitFromResponse(resp, state)
}

func (h *ubiquitiHandlerConfig) fetchCursor(client *http.Client, rl *rate.Limiter, state trackedObjectState) error {
	next := state.Key
	for {
		if err := rl.Wait(h.CTX); err != nil {
			return err
		}
		v := url.Values{}
		if next != "" {
			v.Set("cursor", next)
		}

		u := fmt.Sprintf("https://%s%s", h.Domain, ensureLeadingSlash(h.API))
		if enc := v.Encode(); enc != "" {
			if strings.Contains(u, "?") {
				u = u + "&" + enc
			} else {
				u = u + "?" + enc
			}
		}

		req, _ := http.NewRequest(http.MethodGet, u, nil)
		req.Header.Set("X-API-KEY", h.Token)
		req.Header.Set("Accept", "application/json")

		resp, err := doRequest(client, req, h.CTX)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Site Manager: {data:[...], ...} is common; use data if present
		dataRaw, ok := extractAnyDataRaw(body)
		if !ok {
			// fallback: try treat body as array
			dataRaw = body
		}

		var arr []json.RawMessage
		if err := json.Unmarshal(dataRaw, &arr); err != nil || arr == nil {
			// no usable array; emit raw and stop
			_ = h.emitFromBody(body, state)
			return nil
		}

		var ents []*entry.Entry
		newTS := state.LatestTime
		for _, raw := range arr {
			ts, _ := extractTimestamp(raw)
			if ts.After(newTS) {
				newTS = ts
			}
			ents = append(ents, &entry.Entry{Tag: h.Tag, TS: entry.FromStandard(ts), SRC: h.SRC, Data: raw})
		}
		if len(ents) > 0 {
			if err := h.PROC.ProcessBatchContext(ents, h.CTX); err != nil {
				return err
			}
		}

		// cursor extraction: support {cursor:"..."} or {data:{nextCursor:"..."}}
		// If it’s not present, we will exit.
		var cur string
		var obj struct {
			Cursor     string `json:"cursor"`
			NextCursor string `json:"nextCursor"`
		}
		_ = json.Unmarshal(body, &obj)
		if obj.NextCursor != "" {
			cur = obj.NextCursor
		} else {
			cur = obj.Cursor
		}

		state.LatestTime = newTS
		state.Key = cur
		if err := h.OT.Set("ubiquiti", h.Name, state, false); err != nil {
			lg.Error("failed to update state", log.KVErr(err))
		}

		if len(arr) == 0 || cur == "" {
			break
		}
		next = cur
	}
	return nil
}

func (h *ubiquitiHandlerConfig) fetchTimeWindow(client *http.Client, rl *rate.Limiter, state trackedObjectState) error {
	start := state.LatestTime
	end := time.Now().Add(-2 * time.Minute)

	if err := rl.Wait(h.CTX); err != nil {
		return err
	}

	v := url.Values{}
	v.Set("start", start.Format(time.RFC3339))
	v.Set("end", end.Format(time.RFC3339))

	u := fmt.Sprintf("https://%s%s?%s", h.Domain, ensureLeadingSlash(h.API), v.Encode())

	req, _ := http.NewRequest(http.MethodGet, u, nil)
	req.Header.Set("X-API-KEY", h.Token)
	req.Header.Set("Accept", "application/json")

	resp, err := doRequest(client, req, h.CTX)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return h.emitFromResponse(resp, state)
}

func (h *ubiquitiHandlerConfig) fetchMultiEndpoint(client *http.Client, rl *rate.Limiter, state trackedObjectState) error {
	for _, p := range h.Endpoints {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		lg.Info("Fetching endpoint", log.KV("listener", h.Name), log.KV("path", p))
		orig := h.API
		h.API = p
		if err := h.fetchSingle(client, rl, state); err != nil {
			lg.Error("endpoint fetch error", log.KV("listener", h.Name), log.KV("path", p), log.KVErr(err))
		}
		h.API = orig
	}
	return nil
}

// expand:
//  1) GET list at h.API
//  2) extract IDs from elements in list
//  3) for each ID, call each template in h.Endpoints replacing tokens -> ID
func (h *ubiquitiHandlerConfig) fetchExpand(client *http.Client, rl *rate.Limiter, state trackedObjectState) error {
	// 1) list fetch
	if err := rl.Wait(h.CTX); err != nil {
		return err
	}

	u := fmt.Sprintf("https://%s%s", h.Domain, ensureLeadingSlash(h.API))
	req, _ := http.NewRequest(http.MethodGet, u, nil)
	req.Header.Set("X-API-KEY", h.Token)
	req.Header.Set("Accept", "application/json")

	resp, err := doRequest(client, req, h.CTX)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// Emit list body (but prefer emitting just elements if possible)
	listItems, err := extractAnyDataArray(body)
	if err != nil {
		// still emit raw body for debugging/visibility
		_ = h.emitFromBody(body, state)
		return err
	}

	// Emit each element + collect IDs
	ids := make([]string, 0, len(listItems))
	now := time.Now()
	ents := make([]*entry.Entry, 0, len(listItems))

	for _, raw := range listItems {
		if id := extractExpandID(raw, h.ExpandIDKey); id != "" {
			ids = append(ids, id)
		}
		ents = append(ents, &entry.Entry{
			Tag:  h.Tag,
			TS:   entry.FromStandard(now),
			SRC:  h.SRC,
			Data: raw,
		})
	}

	if len(ents) > 0 {
		if err := h.PROC.ProcessBatchContext(ents, h.CTX); err != nil {
			return err
		}
	}

	// 2) per-id fetch using endpoint templates
	for _, id := range ids {
		for _, tmpl := range h.Endpoints {
			tmpl = strings.TrimSpace(tmpl)
			if tmpl == "" {
				continue
			}
			path := applyExpandTokens(tmpl, id, h.ExpandToken, h.ExpandIDKey)

			if err := rl.Wait(h.CTX); err != nil {
				return err
			}

			u := fmt.Sprintf("https://%s%s", h.Domain, ensureLeadingSlash(path))
			req, _ := http.NewRequest(http.MethodGet, u, nil)
			req.Header.Set("X-API-KEY", h.Token)
			req.Header.Set("Accept", "application/json")

			resp, err := doRequest(client, req, h.CTX)
			if err != nil {
				lg.Error("expand fetch error", log.KV("listener", h.Name), log.KV("path", path), log.KVErr(err))
				continue
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			// Emit response (tries to strip envelopes/wrappers when possible)
			_ = h.emitFromBody(b, state)
		}
	}

	// expand doesn’t advance LatestTime meaningfully; keep it fresh to avoid confusion
	state.LatestTime = time.Now()
	_ = h.OT.Set("ubiquiti", h.Name, state, false)

	return nil
}

// ------------------------------- emit helpers --------------------------------

func (h *ubiquitiHandlerConfig) emitFromResponse(resp *http.Response, state trackedObjectState) error {
	body, _ := io.ReadAll(resp.Body)
	return h.emitFromBody(body, state)
}

func (h *ubiquitiHandlerConfig) emitFromBody(body []byte, state trackedObjectState) error {
	// Prefer emitting "data" if present (Site Manager envelope or Network wrapper),
	// otherwise emit body as-is.
	if dataRaw, ok := extractAnyDataRaw(body); ok && len(dataRaw) > 0 {
		// dataRaw may be array or object
		var arr []json.RawMessage
		if err := json.Unmarshal(dataRaw, &arr); err == nil && arr != nil {
			newTS := state.LatestTime
			var ents []*entry.Entry
			for _, raw := range arr {
				ts, _ := extractTimestamp(raw)
				if ts.After(newTS) {
					newTS = ts
				}
				ents = append(ents, &entry.Entry{Tag: h.Tag, TS: entry.FromStandard(ts), SRC: h.SRC, Data: raw})
			}
			if len(ents) > 0 {
				if err := h.PROC.ProcessBatchContext(ents, h.CTX); err != nil {
					return err
				}
			}
			state.LatestTime = newTS
			_ = h.OT.Set("ubiquiti", h.Name, state, false)
			return nil
		}

		// single object in dataRaw
		ts, _ := extractTimestamp(dataRaw)
		e := &entry.Entry{Tag: h.Tag, TS: entry.FromStandard(ts), SRC: h.SRC, Data: dataRaw}
		if err := h.PROC.ProcessBatchContext([]*entry.Entry{e}, h.CTX); err != nil {
			return err
		}
		state.LatestTime = ts
		_ = h.OT.Set("ubiquiti", h.Name, state, false)
		return nil
	}

	// If body is a top-level array, emit elements
	var arr []json.RawMessage
	if err := json.Unmarshal(body, &arr); err == nil && arr != nil {
		var ents []*entry.Entry
		now := time.Now()
		for _, raw := range arr {
			ents = append(ents, &entry.Entry{Tag: h.Tag, TS: entry.FromStandard(now), SRC: h.SRC, Data: raw})
		}
		if len(ents) > 0 {
			return h.PROC.ProcessBatchContext(ents, h.CTX)
		}
		return nil
	}

	// fallback: emit raw body
	e := &entry.Entry{Tag: h.Tag, TS: entry.FromStandard(time.Now()), SRC: h.SRC, Data: body}
	return h.PROC.ProcessBatchContext([]*entry.Entry{e}, h.CTX)
}

func extractAnyDataRaw(body []byte) (json.RawMessage, bool) {
	// Site Manager envelope: {data: ...}
	var env smEnvelope
	if err := json.Unmarshal(body, &env); err == nil && len(env.Data) > 0 {
		return env.Data, true
	}
	// Network wrapper (common): {"data": ...}
	var w struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &w); err == nil && len(w.Data) > 0 {
		return w.Data, true
	}
	return nil, false
}

func extractAnyDataArray(body []byte) ([]json.RawMessage, error) {
	if dataRaw, ok := extractAnyDataRaw(body); ok && len(dataRaw) > 0 {
		var arr []json.RawMessage
		if err := json.Unmarshal(dataRaw, &arr); err == nil && arr != nil {
			return arr, nil
		}
		// sometimes list endpoints can return an object; not expandable
		return nil, errors.New("data field is not an array")
	}

	// allow top-level array
	var arr []json.RawMessage
	if err := json.Unmarshal(body, &arr); err == nil && arr != nil {
		return arr, nil
	}
	return nil, errors.New("no data array found")
}

func extractExpandID(raw json.RawMessage, preferredKey string) string {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}

	// preferred first
	if preferredKey = strings.TrimSpace(preferredKey); preferredKey != "" {
		if v, ok := m[preferredKey]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}

	// common fallbacks
	keys := []string{
		"id",
		"deviceId",
		"clientId",
		"networkId",
		"wifiBroadcastId",
		"wanId",
		"vpnId",
		"radiusProfileId",
		"voucherId",
	}
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}

	return ""
}

func applyExpandTokens(tmpl, id, expandToken, expandIDKey string) string {
	out := tmpl

	// replace configured token + a few common tokens
	tokens := []string{
		expandToken,
		"{id}",
		"{deviceId}",
		"{clientId}",
		"{networkId}",
		"{wifiBroadcastId}",
		"{wanId}",
		"{vpnId}",
		"{radiusProfileId}",
		"{voucherId}",
	}
	if expandIDKey = strings.TrimSpace(expandIDKey); expandIDKey != "" {
		tokens = append(tokens, "{"+expandIDKey+"}")
	}

	seen := map[string]struct{}{}
	for _, tok := range tokens {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if _, ok := seen[tok]; ok {
			continue
		}
		seen[tok] = struct{}{}
		out = strings.ReplaceAll(out, tok, id)
	}
	return out
}

// ------------------------------- http helper ---------------------------------

func doRequest(client *http.Client, req *http.Request, ctx context.Context) (*http.Response, error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		resp, err := client.Do(req.WithContext(ctx))
		if err != nil {
			lg.Error("request error; backing off", log.KVErr(err))
			if quitableSleep(ctx, 5*time.Second) {
				return nil, err
			}
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			ra := parseRetryAfter(resp.Header.Get(retryAfterHeaderName))
			lg.Info("rate limited; sleeping", log.KV("retryAfter", ra.String()))
			if quitableSleep(ctx, ra) {
				resp.Body.Close()
				return nil, errors.New("context canceled during rate limit sleep")
			}
			resp.Body.Close()
			continue
		}

		if resp.StatusCode >= 500 {
			lg.Info("server error; retrying", log.KV("status", resp.Status))
			resp.Body.Close()
			if quitableSleep(ctx, 5*time.Second) {
				return nil, fmt.Errorf("canceled while backing off: %s", resp.Status)
			}
			continue
		}

		// For 4xx (other than 429), return response as-is; caller may emit body for visibility.
		return resp, nil
	}
}

func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 5 * time.Second
	}
	// integer seconds
	if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	// HTTP-date
	if t, err := http.ParseTime(v); err == nil {
		return time.Until(t)
	}
	return 5 * time.Second
}

func ensureLeadingSlash(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	if p[0] != '/' {
		return "/" + p
	}
	return p
}
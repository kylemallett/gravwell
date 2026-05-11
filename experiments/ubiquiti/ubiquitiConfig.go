package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// UbiquitiConf supports both:
//
//  1) UniFi Site Manager (cloud): Domain=api.ui.com, API=/v1/{hosts|sites|devices}
//  2) UniFi Network (local controller): Domain=<udm/ck ip>, API=/proxy/network/integration/v1/...
//
// Modes:
//   - single:     one GET to API
//   - cursor:     repeats GET to API with cursor=<state.Key> (your handler adds cursor param)
//   - timewindow: GET to API with start/end query params (your handler adds start/end)
//   - router:     iterates Endpoints (list of paths) and calls single for each
//   - expand:     list GET to API, extracts an id from each element, then calls each Endpoints template
//                with token replacement (default token "{id}").
//
// NOTE: For expand, you still hardcode siteId in API/Endpoints. Expand only fills per-object IDs.
type UbiquitiConf struct {
	Domain string // default: api.ui.com
	Token  string // API key used in X-API-KEY header

	// API is the main request path. For expand, this should be the LIST endpoint.
	API string

	// Endpoints is:
	//   - router: list of paths to fetch each cycle
	//   - expand: list of template paths to fetch per-expanded id
	//
	// Example (expand devices):
	//   API="/proxy/network/integration/v1/sites/<siteId>/devices"
	//   Endpoints=[
	//     "/proxy/network/integration/v1/sites/<siteId>/devices/{deviceId}",
	//     "/proxy/network/integration/v1/sites/<siteId>/devices/{deviceId}/statistics/latest",
	//   ]
	//
	// If using expand generically, prefer "{id}" as token and set ExpandToken accordingly.
	Endpoints []string

	// "single", "cursor", "timewindow", "router", "expand"
	Mode string

	// Used by cursor/timewindow logic (seed) and any state tracking.
	StartTime time.Time

	// Tag-Name in config
	Tag_Name string

	// Preprocessor pipeline names
	Preprocessor []string

	// Requests per minute
	RateLimit int

	// Optional: allow per-listener TLS skip verify for local controllers (recommended for UDM/CK).
	// This requires wiring into your ubiquiti.go http.Client transport.
	Insecure bool

	// Expansion settings (Mode="expand")
	//
	// ExpandToken: token to replace in Endpoints templates. Default "{id}".
	// ExpandIDKey: JSON field name to extract from each list element. Default "id".
	//
	// Your expander can also optionally try common alternates (deviceId/clientId/etc) in code,
	// but this gives you control without changing code.
	ExpandToken string
	ExpandIDKey  string
}

func (c cfgType) UbiquitiVerify() error {
	for name, v := range c.UbiquitiConf {
		if v == nil {
			return fmt.Errorf("UbiquitiConf %q: nil stanza", name)
		}
		if strings.TrimSpace(v.Tag_Name) == "" {
			return fmt.Errorf("UbiquitiConf %q: Tag-Name missing", name)
		}
		if strings.TrimSpace(v.Token) == "" {
			return fmt.Errorf("UbiquitiConf %q: Token (X-API-KEY) missing", name)
		}

		mode := strings.ToLower(strings.TrimSpace(v.Mode))
		if mode == "" {
			mode = "single"
			v.Mode = mode
		} else {
			v.Mode = mode
		}

		switch mode {
		case "single", "cursor", "timewindow", "router", "expand":
		default:
			return fmt.Errorf("UbiquitiConf %q: invalid Mode %q", name, v.Mode)
		}

		// Default domain and API path (safe for Site Manager)
		if strings.TrimSpace(v.Domain) == "" {
			v.Domain = "api.ui.com"
		}

		// Defaults for API:
		// - router can omit API (it uses Endpoints list)
		// - expand MUST have API (list endpoint)
		if strings.TrimSpace(v.API) == "" {
			switch mode {
			case "router":
				// ok
			case "expand":
				return fmt.Errorf("UbiquitiConf %q: API (list endpoint) required for Mode=expand", name)
			default:
				v.API = "/v1/hosts"
			}
		}

		// Defaults for StartTime / RateLimit
		if v.StartTime.IsZero() {
			v.StartTime = time.Now().Add(-24 * time.Hour)
		}
		if v.RateLimit <= 0 {
			v.RateLimit = 100
		}

		// Router/Expand require endpoints list
		if mode == "router" || mode == "expand" {
			if len(v.Endpoints) == 0 {
				return fmt.Errorf("UbiquitiConf %q: Endpoints required for Mode=%s", name, mode)
			}
			for i, ep := range v.Endpoints {
				if strings.TrimSpace(ep) == "" {
					return fmt.Errorf("UbiquitiConf %q: Endpoints[%d] empty", name, i)
				}
			}
		}

		// Expand defaults
		if mode == "expand" {
			if strings.TrimSpace(v.ExpandToken) == "" {
				v.ExpandToken = "{id}"
			}
			if strings.TrimSpace(v.ExpandIDKey) == "" {
				v.ExpandIDKey = "id"
			}
			if !strings.Contains(v.ExpandToken, "{") || !strings.Contains(v.ExpandToken, "}") {
				return fmt.Errorf("UbiquitiConf %q: ExpandToken must look like \"{id}\" (got %q)", name, v.ExpandToken)
			}
		}

		// write back any defaults
		c.UbiquitiConf[name] = v
	}

	// If there are no stanzas, that's ok; Tags() will fail later if no tags exist.
	return nil
}

// Optional helper for callers that want a simple “is this config empty” check.
func (c *cfgType) HasUbiquiti() bool {
	return c != nil && len(c.UbiquitiConf) > 0
}

// Backwards-compat guard: if anyone still tries to configure Endpoints as a map
// via overlays or older templates, surface a clearer error where possible.
var errUbiquitiEndpointsType = errors.New("UbiquitiConf Endpoints must be a list: Endpoints=[\"/path1\",\"/path2\"]")
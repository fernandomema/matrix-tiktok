package libtiktok

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-resty/resty/v2"
	"golang.org/x/net/html"
)

const DefaultUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"

// Default TikTok IM API and WebSocket hosts (no region suffix).
const (
	DefaultIMAPIHost = "im-api.tiktok.com"
	DefaultIMWSHost  = "im-ws.tiktok.com"
)

// ClientConfig holds optional overrides for the TikTok API hosts and HTTP
// user-agent. Zero values fall back to built-in defaults.
type ClientConfig struct {
	// IMAPIHost is the hostname (no scheme) for the TikTok IM REST API.
	// Default: im-api.tiktok.com
	IMAPIHost string
	// IMWSHost is the hostname (no scheme) for the TikTok IM WebSocket.
	// Default: im-ws.tiktok.com
	IMWSHost string
	// UserAgent overrides the default HTTP User-Agent header.
	UserAgent string
}

func (c ClientConfig) imAPIHost() string {
	if c.IMAPIHost != "" {
		return c.IMAPIHost
	}
	return DefaultIMAPIHost
}

func (c ClientConfig) imWSHost() string {
	if c.IMWSHost != "" {
		return c.IMWSHost
	}
	return DefaultIMWSHost
}

func (c ClientConfig) userAgent() string {
	if c.UserAgent != "" {
		return c.UserAgent
	}
	return DefaultUserAgent
}

type Client struct {
	// r is the client for www.tiktok.com
	r *resty.Client
	// rIA is the client for the IM API specifically
	rIA *resty.Client
	// cfg holds resolved host/UA settings
	cfg ClientConfig
}

type MessagesUniversalData map[string]any

func (m MessagesUniversalData) getAppContext() (map[string]any, error) {
	defaultScope, ok := m["__DEFAULT_SCOPE__"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("__DEFAULT_SCOPE__ not found or wrong type")
	}

	appContext, ok := defaultScope["webapp.app-context"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("webapp.app-context not found or wrong type")
	}

	return appContext, nil
}

// getMessagesUniversalData fetches /messages, extracts the
// #__UNIVERSAL_DATA_FOR_REHYDRATION__ script tag, and returns its contents
// as a parsed JSON map.
func (c *Client) getMessagesUniversalData() (MessagesUniversalData, error) {
	resp, err := c.r.R().
		SetHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8").
		Get("/messages")
	if err != nil {
		return nil, fmt.Errorf("get /messages: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("get /messages: unexpected status %d", resp.StatusCode())
	}

	rawJSON, err := extractUniversalData(resp.String())
	if err != nil {
		return nil, fmt.Errorf("extract universal data: %w", err)
	}

	var result map[string]any
	if err := json.Unmarshal([]byte(rawJSON), &result); err != nil {
		return nil, fmt.Errorf("parse universal data JSON: %w", err)
	}

	return result, nil
}

// extractUniversalData parses the HTML body and returns the raw JSON string
// contained in the <script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"> tag.
func extractUniversalData(body string) (string, error) {
	doc, err := html.Parse(strings.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("parse HTML: %w", err)
	}

	var content string
	var walk func(*html.Node) bool
	walk = func(n *html.Node) bool {
		if n.Type == html.ElementNode && n.Data == "script" {
			for _, attr := range n.Attr {
				if attr.Key == "id" && attr.Val == "__UNIVERSAL_DATA_FOR_REHYDRATION__" {
					if n.FirstChild != nil {
						content = strings.TrimSpace(n.FirstChild.Data)
						return true
					}
				}
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			if walk(child) {
				return true
			}
		}
		return false
	}
	walk(doc)

	if content == "" {
		return "", fmt.Errorf("script#__UNIVERSAL_DATA_FOR_REHYDRATION__ not found or empty")
	}
	return content, nil
}

// NewClient creates a TikTok IM client using the provided cookie string and
// optional ClientConfig overrides. Pass a zero-value ClientConfig to use all
// built-in defaults.
func NewClient(cookieString string, cfg ClientConfig) *Client {
	ua := cfg.userAgent()
	imAPIBase := "https://" + cfg.imAPIHost()

	r := resty.New()
	r.SetHeader("Cookie", cookieString)
	r.SetHeader("User-Agent", ua)
	r.SetHeader("Accept-Language", "en-US,en;q=0.9")
	r.SetBaseURL("https://www.tiktok.com")

	rIA := resty.New()
	rIA.SetHeader("Cookie", cookieString)
	rIA.SetHeader("User-Agent", ua)
	rIA.SetHeader("Accept-Language", "en-US,en;q=0.9")
	rIA.SetHeader("Referer", "https://www.tiktok.com/")
	rIA.SetBaseURL(imAPIBase)
	return &Client{
		r:   r,
		rIA: rIA,
		cfg: cfg,
	}
}

func extractCookie(cookieStr, name string) string {
	for _, part := range strings.Split(cookieStr, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 && strings.TrimSpace(kv[0]) == name {
			return strings.TrimSpace(kv[1])
		}
	}
	return ""
}

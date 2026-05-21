package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"

	tiktokpb "github.com/httpjamesm/matrix-tiktok/pkg/libtiktok/pb"
	"github.com/httpjamesm/matrix-tiktok/pkg/libtiktok"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func extractCookie(cookieStr, name string) string {
	for _, part := range strings.Split(cookieStr, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 && strings.TrimSpace(kv[0]) == name {
			return strings.TrimSpace(kv[1])
		}
	}
	return ""
}

func getWID(cookies string) (string, error) {
	req, _ := http.NewRequest("GET", "https://www.tiktok.com/messages", nil)
	req.Header.Set("Cookie", cookies)
	req.Header.Set("User-Agent", libtiktok.DefaultUserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	re := regexp.MustCompile(`id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)</script>`)
	m := re.FindSubmatch(body)
	if m == nil {
		return "", fmt.Errorf("no rehydration data")
	}
	var data map[string]any
	json.Unmarshal(m[1], &data)
	scope := data["__DEFAULT_SCOPE__"].(map[string]any)
	appCtx := scope["webapp.app-context"].(map[string]any)
	wid, _ := appCtx["wid"].(string)
	return wid, nil
}

func tryCombo(cookies string, payload []byte) {
	url := "https://im-api.tiktok.com/v1/message/get_by_user_combo?aid=1988&version_code=1.0.0&app_name=tiktok_web&device_platform=web_pc"
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(payload)))
	req.Header.Set("Cookie", cookies)
	req.Header.Set("User-Agent", libtiktok.DefaultUserAgent)
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Origin", "https://www.tiktok.com")
	req.Header.Set("Referer", "https://www.tiktok.com/")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("HTTP error: %v\n", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	fmt.Printf("HTTP %d | response bytes=%d\n", resp.StatusCode, len(body))
	if len(body) > 0 {
		fmt.Printf("Raw hex dump (first 256 bytes):\n%s\n", hex.Dump(body[:min(256, len(body))]))
	}

	var pbResp tiktokpb.InboxResponse
	if err := proto.Unmarshal(body, &pbResp); err != nil {
		fmt.Printf("proto.Unmarshal error: %v\n", err)
	} else {
		fmt.Printf("InboxResponse: message_type=%d sub_command=%d status=%d message=%q\n",
			pbResp.GetMessageType(), pbResp.GetSubCommand(), pbResp.GetStatus(), pbResp.GetMessage())

		if p := pbResp.GetPayload(); p != nil {
			ul := p.GetUserInitList()
			fmt.Printf("  payload.user_init_list(203): convs=%d entries=%d\n",
				len(ul.GetConversations()), len(ul.GetEntries()))

			// Scan unknown fields for field 204
			unknown := p.ProtoReflect().GetUnknown()
			fmt.Printf("  payload unknown bytes (%d): %s\n", len(unknown), hex.EncodeToString(unknown))
			for len(unknown) > 0 {
				num, typ, n := protowire.ConsumeTag(unknown)
				if n < 0 { break }
				unknown = unknown[n:]
				if typ == protowire.BytesType {
					val, n2 := protowire.ConsumeBytes(unknown)
					if n2 < 0 { break }
					unknown = unknown[n2:]
					fmt.Printf("  unknown field %d (bytes, len=%d)\n", num, len(val))
					if num == 204 {
						// val = {field1 → inner}; unwrap field1 then unmarshal as InboxConversationList.
						inner := unwrapField1Bytes(val)
						fmt.Printf("    field204 raw val len=%d, inner len=%d\n", len(val), len(inner))
						if inner != nil {
							var list tiktokpb.InboxConversationList
							if err2 := proto.Unmarshal(inner, &list); err2 != nil {
								fmt.Printf("    unmarshal field204 inner error: %v\n", err2)
							} else {
								fmt.Printf("    field204 InboxConversationList: convs=%d entries=%d\n",
									len(list.GetConversations()), len(list.GetEntries()))
								for i, c := range list.GetConversations() {
									if i >= 5 { fmt.Printf("    ... (%d more)\n", len(list.GetConversations())-5); break }
									fmt.Printf("    conv[%d]: id=%q type=%d\n", i, c.GetConversationId(), c.GetConversationType())
								}
							}
						}
					}
				} else if typ == protowire.VarintType {
					v, n2 := protowire.ConsumeVarint(unknown)
					if n2 < 0 { break }
					unknown = unknown[n2:]
					fmt.Printf("  unknown field %d (varint=%d)\n", num, v)
				} else {
					fmt.Printf("  unknown field %d type=%d (unsupported)\n", num, typ)
					break
				}
			}
		} else {
			fmt.Println("  payload is nil — check unknown fields on InboxResponse:")
			unknown := pbResp.ProtoReflect().GetUnknown()
			fmt.Printf("  InboxResponse unknown bytes (%d): %s\n", len(unknown), hex.EncodeToString(unknown))
		}
	}
}

func main() {
	cookies := os.Getenv("TIKTOK_COOKIES")
	if cookies == "" {
		fmt.Fprintln(os.Stderr, "set TIKTOK_COOKIES"); os.Exit(1)
	}

	wid, err := getWID(cookies)
	if err != nil {
		fmt.Println("getWID error:", err); os.Exit(1)
	}
	msToken := extractCookie(cookies, "msToken")
	verifyFP := extractCookie(cookies, "s_v_web_id")
	fmt.Printf("wid=%s msToken_len=%d verifyFP=%s\n\n", wid, len(msToken), verifyFP)

	payload, err := libtiktok.BuildInboxPayloadForTest(wid, msToken, verifyFP, 0)
	if err != nil {
		fmt.Printf("build payload error: %v\n", err); os.Exit(1)
	}
	fmt.Printf("Payload (%d bytes):\n%s\n", len(payload), hex.Dump(payload[:min(128, len(payload))]))
	tryCombo(cookies, payload)
}

func min(a, b int) int {
	if a < b { return a }
	return b
}

func unwrapField1Bytes(data []byte) []byte {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 { return nil }
		data = data[n:]
		if typ == protowire.BytesType {
			val, n2 := protowire.ConsumeBytes(data)
			if n2 < 0 { return nil }
			if num == 1 { return val }
			data = data[n2:]
		} else if typ == protowire.VarintType {
			_, n2 := protowire.ConsumeVarint(data)
			if n2 < 0 { return nil }
			data = data[n2:]
		} else {
			return nil
		}
	}
	return nil
}

package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	tiktokpb "github.com/httpjamesm/matrix-tiktok/pkg/libtiktok/pb"
	"github.com/httpjamesm/matrix-tiktok/pkg/libtiktok"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func protoStr(s string) *string { return &s }
func protoU64(v uint64) *uint64 { return &v }

func buildMeta(deviceID, msToken, verifyFP string) []*tiktokpb.MetadataKV {
	return []*tiktokpb.MetadataKV{
		{Key: protoStr("aid"), Value: protoStr("1988")},
		{Key: protoStr("app_name"), Value: protoStr("tiktok_web")},
		{Key: protoStr("device_id"), Value: protoStr(deviceID)},
		{Key: protoStr("region"), Value: protoStr("GB")},
		{Key: protoStr("cookie"), Value: protoStr("msToken=" + msToken)},
		{Key: protoStr("referer"), Value: protoStr("https://www.tiktok.com/")},
		{Key: protoStr("ms_token"), Value: protoStr(msToken)},
		{Key: protoStr("verify_fp"), Value: protoStr(verifyFP)},
	}
}

func tryConv(cookies, convID string, sourceID uint64, msToken, verifyFP string) {
	msg := &tiktokpb.GetByConversationRequest{
		MessageType:    protoU64(301),
		SubCommand:     protoU64(1),
		ClientVersion:  protoStr("1.6.0"),
		PlatformFlag:   protoU64(3),
		Reserved_6:     protoU64(0),
		GitHash:        protoStr(""),
		ClientPlatform: protoStr("web"),
		Metadata:       buildMeta("7619729754845939222", msToken, verifyFP),
		FinalFlag:      protoU64(1),
		Payload: &tiktokpb.GetByConversationRequestPayload{
			Query: &tiktokpb.GetByConversationQuery{
				ConversationId: protoStr(convID),
				Direction:      protoU64(1),
				SourceId:       protoU64(sourceID),
				Reserved_4:     protoU64(1),
				CursorTsUs:     protoU64(0),
				Count:          protoU64(20),
			},
		},
	}
	payload, _ := proto.Marshal(msg)

	url := "https://im-api.tiktok.com/v1/message/get_by_conversation?aid=1988&version_code=1.0.0&app_name=tiktok_web&device_platform=web_pc"
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(payload)))
	req.Header.Set("Cookie", cookies)
	req.Header.Set("User-Agent", libtiktok.DefaultUserAgent)
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Origin", "https://www.tiktok.com")
	req.Header.Set("Referer", "https://www.tiktok.com/")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("err:", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("HTTP %d | bytes=%d\n", resp.StatusCode, len(body))
	if len(body) > 0 {
		fmt.Printf("Hex:\n%s\n", hex.Dump(body[:min(128, len(body))]))
	}

	var r tiktokpb.GetByConversationResponse
	if err := proto.Unmarshal(body, &r); err != nil {
		fmt.Println("unmarshal err:", err)
	} else {
		fmt.Printf("status=%d entries=%d\n", r.GetStatus(), len(r.GetPayload().GetConversation().GetEntries()))
	}

	fmt.Println("Top-level fields:")
	scanFields(body)
}

func scanFields(data []byte) {
	i := 0
	for i < len(data) {
		num, typ, n := protowire.ConsumeTag(data[i:])
		if n < 0 {
			break
		}
		i += n
		switch typ {
		case protowire.VarintType:
			v, n2 := protowire.ConsumeVarint(data[i:])
			if n2 < 0 {
				return
			}
			fmt.Printf("  field %d varint=%d\n", num, v)
			i += n2
		case protowire.BytesType:
			v, n2 := protowire.ConsumeBytes(data[i:])
			if n2 < 0 {
				return
			}
			i += n2
			try := func() string {
				s := string(v)
				for _, c := range s {
					if c < 32 && c != '\n' && c != '\t' {
						return ""
					}
				}
				return s
			}()
			if try != "" && len(try) < 80 {
				fmt.Printf("  field %d string=%q\n", num, try)
			} else {
				fmt.Printf("  field %d bytes(len=%d): %s\n", num, len(v), hex.EncodeToString(v))
			}
		default:
			fmt.Printf("  field %d wt=%d UNKNOWN\n", num, typ)
			return
		}
	}
}

func extractCookie(s, name string) string {
	for _, p := range strings.Split(s, ";") {
		kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(kv) == 2 && strings.TrimSpace(kv[0]) == name {
			return strings.TrimSpace(kv[1])
		}
	}
	return ""
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	cookies := os.Getenv("TIKTOK_COOKIES")
	if cookies == "" {
		fmt.Fprintln(os.Stderr, "set TIKTOK_COOKIES")
		os.Exit(1)
	}
	msToken := extractCookie(cookies, "msToken")
	verifyFP := extractCookie(cookies, "s_v_web_id")

	// Test conversation that returns 0 messages — source_id from bridge log is 7461368418211463430
	convID := "0:1:6750007489691272198:7341562143077204997"
	for _, sourceID := range []uint64{7461368418211463430, 7341562143077204997, 6750007489691272198, 0} {
		fmt.Printf("\n=== conv=%s source_id=%d ===\n", convID, sourceID)
		tryConv(cookies, convID, sourceID, msToken, verifyFP)
	}
}

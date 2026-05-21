package libtiktok

	import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	tiktokpb "github.com/httpjamesm/matrix-tiktok/pkg/libtiktok/pb"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// errSkipSyncedMessage is returned by parseMessageEntry for rows the bridge
// must not turn into Matrix events (recalled / invisible placeholders).
var errSkipSyncedMessage = errors.New("skip synced message")

type Conversation struct {
	ID           string   // 0:1:X:Y
	SourceID     uint64   // 5: from get_by_user_init
	Participants []string // user IDs
	// Name is the explicit conversation title when TikTok provides one.
	// Group chats currently expose this under detail.core.title; DMs leave it empty.
	Name string
	// ConversationType is the wire conversation_type value: 1 for DMs, 2 for group chats.
	ConversationType uint64
	// Muted is the per-user notification state from detail.state.attributes["a:conv_set_notification"].
	// A nil value means the inbox bootstrap did not include a known mute state.
	Muted *bool
}

type Message struct {
	ServerID        uint64
	ConvID          string
	ClientMessageID string
	SenderID        string
	Type            string // "text", "image", "video", "sticker"
	MessageSubtype  string
	Text            string
	MediaURL        string
	ThumbnailURL    string
	// MediaDecryptKey is currently used by private_image messages, whose CDN
	// blobs are encrypted with AES-256-GCM.
	MediaDecryptKey string
	MimeType        string
	MediaWidth      int
	MediaHeight     int
	MediaDurationMs int
	TimestampMs     int64
	// TimestampUs is wire field 4 (timestamp_us) on ConversationMessageEntry; used
	// for mark_read read_message_index (same scale as get_by_conversation rows).
	TimestampUs uint64
	Reactions   []Reaction
	// ReplyToServerID is the parent message's server_message_id when this DM is a reply (aweType 703).
	ReplyToServerID uint64
	// ReplyQuotedText is a short plain-text preview of the parent message from message_reply (field 2 JSON), when present.
	ReplyQuotedText string
	// SendChainID is TikTok inner wire field 5; copy into send body field 3 when replying from Matrix.
	SendChainID uint64
	// SenderSecUID is the sender's sec_uid (wire field 14) for building outbound reply reference JSON.
	SenderSecUID string
	// CursorTsUs is field 25 on the wire row; used as parent_cursor_ts_us on outbound replies.
	CursorTsUs uint64
	// RawContentJSON is the original field-8 JSON body bytes (for round-tripping refmsg content on send).
	RawContentJSON []byte
}

// Reaction represents a single emoji reaction on a message and the users who sent it.
// The Emoji field holds the raw emoji string (or text alias) after the "e:" prefix is stripped.
// For example, field-1 value "e:❤️" becomes Emoji "❤️", and "e:love" becomes Emoji "love".
type Reaction struct {
	Emoji   string   // emoji character(s) or text name, e.g. "❤️" or "love"
	UserIDs []string // IDs of users who reacted with this emoji
}

const (
	// inboxURL is the unified combo inbox endpoint on im-api.tiktok.com (absolute URL used in Post).
	inboxURL     = "https://im-api.tiktok.com/v1/message/get_by_user_combo"
	inboxV2URL   = "https://im-api.tiktok.com/v2/message/get_by_user_init"
	getByConvURL = "https://im-api.tiktok.com/v1/message/get_by_conversation"
	imAID        = "1988"
)

// isUnicodeEmoji reports whether s contains at least one non-ASCII rune,
// indicating it is a real Unicode emoji glyph rather than a plain-text alias.
func isUnicodeEmoji(s string) bool {
	for _, r := range s {
		if r > 0x7F {
			return true
		}
	}
	return false
}

// deduplicateReactions collapses reaction entries that share an identical set
// of reacting users, keeping the entry whose Emoji contains non-ASCII runes
// (i.e. an actual unicode emoji glyph) over a plain-text alias.
//
// TikTok encodes each reaction twice on the wire – once as the emoji
// character(s) (e.g. "❤️") and once as a text alias (e.g. "love").
// Because both entries carry exactly the same UserIDs, grouping by that
// fingerprint reliably collapses the duplicates without needing an
// alias-to-emoji lookup table.
func deduplicateReactions(in []Reaction) []Reaction {
	if len(in) <= 1 {
		return in
	}

	type slot struct {
		idx     int
		isEmoji bool
	}
	seen := make(map[string]slot, len(in))
	out := make([]Reaction, 0, len(in))

	for _, r := range in {
		key := strings.Join(r.UserIDs, "\x00")
		uni := isUnicodeEmoji(r.Emoji)
		if s, ok := seen[key]; ok {
			if uni && !s.isEmoji {
				out[s.idx] = r
				seen[key] = slot{idx: s.idx, isEmoji: true}
			}
		} else {
			seen[key] = slot{idx: len(out), isEmoji: uni}
			out = append(out, r)
		}
	}
	return out
}

// parseMessageContent decodes the JSON content blob (field 8 in both the REST
// get-by-conversation response and the WebSocket push frame) and returns the
// (msgType, text, mediaURL, mimeType) fields for a Message.
//
// Known aweType values:
//
//	0, 700 → "text"  (REST API uses 0; WebSocket push uses 700)
//	703    → "text"  (reply; same text field; parent id is protobuf message_reply, field 1)
//	800    → "video" (shared TikTok post)
//	810    → "video" (shared TikTok post, photomode / photoset variant)
//	1      → "video" (shared TikTok post with source_type=2; appears in DM history)
func parseMessageContent(ctx context.Context, c *Client, contentBytes []byte) (msgType, text, mediaURL, mimeType string) {
	if len(contentBytes) == 0 {
		return
	}
	content, err := parseContentJSONObject(contentBytes)
	if err != nil {
		return
	}
	if stickerURL, stickerText, ok := parseStickerFromContentJSON(content, contentBytes); ok {
		msgType = "sticker"
		text = stickerText
		mediaURL = stickerURL
		mimeType = guessStickerMIMEFromURL(stickerURL)
		if text == "" {
			text = "[sticker]"
		}
		return
	}
	// Media-only rows often ship placeholder JSON like {"hack":"1"} with no aweType.
	// A missing key must not be treated as aweType 0 — that produced Type "text" with
	// an empty body and bridged a stray empty Matrix m.text next to image/video.
	rawAwe, hasAwe := content["aweType"]
	if !hasAwe || rawAwe == nil {
		// Streak / system notification messages arrive without aweType.
		// They carry a human-readable string in "fallback_tips" or "tips".
		// {"hack":"1"} rows are empty placeholders — ignore those.
		if tips, ok := content["fallback_tips"].(string); ok && tips != "" {
			return "text", tips, "", ""
		}
		if tips, ok := content["tips"].(string); ok && tips != "" {
			// Strip {{N}} placeholders used for template buttons.
			text = tips
			for i := 1; i <= 9; i++ {
				text = strings.ReplaceAll(text, fmt.Sprintf(" {{%d}}", i), "")
			}
			return "text", text, "", ""
		}
		return "", "", "", ""
	}
	aweTypeF, ok := rawAwe.(float64)
	if !ok {
		return "", "", "", ""
	}
	switch int(aweTypeF) {
	case 0, 700, 703:
		msgType = "text"
		if value, ok := content["text"].(string); ok {
			text = value
		}
	case 1, 800, 810:
		msgType = "video"
		if itemID, ok := content["itemId"].(string); ok && itemID != "" {
			if uid, ok := content["uid"].(string); ok && uid != "" {
				if user, err := c.GetUser(ctx, uid); err == nil && user.UniqueID != "" {
					mediaURL = "https://www.tiktok.com/@" + user.UniqueID + "/video/" + itemID
				}
			}
		}
		if value, ok := content["content_title"].(string); ok {
			text = value
		}
	case 103301:
		// Group invite: sender is inviting the recipient to a group chat.
		msgType = "text"
		groupName := ""
		if group, ok := content["group"].(map[string]interface{}); ok {
			groupName, _ = group["name"].(string)
		}
		inviterName, _ := content["inviter_name"].(string)
		if groupName != "" && inviterName != "" {
			text = fmt.Sprintf("%s invited you to the group \"%s\"", inviterName, groupName)
		} else if groupName != "" {
			text = fmt.Sprintf("Invited you to the group \"%s\"", groupName)
		} else {
			text = "Group chat invitation"
		}
	default:
		msgType = fmt.Sprintf("type_%d", int(aweTypeF))
		if value, ok := content["text"].(string); ok {
			text = value
		}
		zerolog.Ctx(ctx).Warn().
			Int("awe_type", int(aweTypeF)).
			RawJSON("content", contentBytes).
			Msg("Received TikTok message with unrecognised aweType — please open an issue")
	}
	return
}

// parseReplyQuotedTextFromWire extracts the inner chat "text" from TikTok's message_reply
// quoted_context_json blob (field 2): outer JSON with refmsg_content / content holding a nested JSON body.
func parseReplyQuotedTextFromWire(quotedContextJSON []byte) string {
	if len(quotedContextJSON) == 0 {
		return ""
	}
	var outer struct {
		Content       string `json:"content"`
		RefmsgContent string `json:"refmsg_content"`
	}
	if err := json.Unmarshal(quotedContextJSON, &outer); err != nil {
		return ""
	}
	raw := outer.RefmsgContent
	if raw == "" {
		raw = outer.Content
	}
	if raw == "" {
		return ""
	}
	var inner struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(raw), &inner); err != nil {
		return ""
	}
	return inner.Text
}

type metaKV struct{ k, v string }

func buildMetadata(deviceID, msToken, verifyFP string) []metaKV {
	pairs := []metaKV{
		{"aid", imAID},
		{"app_name", "tiktok_web"},
		{"channel", "web"},
		{"device_platform", "web_pc"},
		{"device_id", deviceID},
		{"region", "GB"},
		{"priority_region", "GB"},
		{"os", "mac"},
		{"referer", "https://www.tiktok.com/"},
		{"root_referer", ""},
		{"cookie_enabled", "true"},
		{"screen_width", "1800"},
		{"screen_height", "1169"},
		{"browser_language", "en-US"},
		{"browser_platform", "MacIntel"},
		{"browser_name", "Mozilla"},
		// The web client mirrors the full UA string here, not just the version token.
		{"browser_version", DefaultUserAgent},
		{"browser_online", "true"},
	}
	if verifyFP != "" {
		pairs = append(pairs, metaKV{"verifyFp", verifyFP})
	}
	pairs = append(pairs,
		metaKV{"app_language", "en"},
		metaKV{"webcast_language", "en"},
		metaKV{"tz_name", "Europe/London"},
		metaKV{"is_page_visible", "true"},
		metaKV{"focus_state", "true"},
		metaKV{"is_fullscreen", "false"},
		metaKV{"history_len", "2"},
		metaKV{"user_is_login", "true"},
		metaKV{"data_collection_enabled", "true"},
		metaKV{"from_appID", imAID},
		metaKV{"locale", "en"},
		metaKV{"user_agent", DefaultUserAgent},
	)
	if msToken != "" {
		pairs = append(pairs, metaKV{"Web-Sdk-Ms-Token", msToken})
	}
	return pairs
}

// BuildInboxPayloadForTest is exported only for debug tooling.
func BuildInboxPayloadForTest(deviceID, msToken, verifyFP string, subCommand uint64) ([]byte, error) {
	return buildInboxPayload(deviceID, msToken, verifyFP, 0)
}

// buildComboPayloadField204 encodes the field-204 bytes for the get_by_user_combo
// InboxRequestPayload. The structure mirrors the browser capture:
//
//	field 204 → field 1 → {field1=0, field2=cursorTsUs, field3=limit, field4=8}
func buildComboPayloadField204(cursorTsUs, limit uint64) []byte {
	// Innermost: fields 1,2,3,4
	var inner []byte
	inner = protowire.AppendTag(inner, 1, protowire.VarintType)
	inner = protowire.AppendVarint(inner, 0)
	inner = protowire.AppendTag(inner, 2, protowire.VarintType)
	inner = protowire.AppendVarint(inner, cursorTsUs)
	inner = protowire.AppendTag(inner, 3, protowire.VarintType)
	inner = protowire.AppendVarint(inner, limit)
	inner = protowire.AppendTag(inner, 4, protowire.VarintType)
	inner = protowire.AppendVarint(inner, 8)

	// field 1 of GetUserComboRequestBody wrapping inner
	var mid []byte
	mid = protowire.AppendTag(mid, 1, protowire.BytesType)
	mid = protowire.AppendBytes(mid, inner)

	// field 204 of InboxRequestPayload (injected as unknown fields)
	var out []byte
	out = protowire.AppendTag(out, 204, protowire.BytesType)
	out = protowire.AppendBytes(out, mid)
	return out
}

func buildInboxPayload(deviceID, msToken, verifyFP string, cursorTsUs uint64) ([]byte, error) {
	payload := &tiktokpb.InboxRequestPayload{}
	// Inject field-204 (combo cursor/limit) as unknown fields so proto.Marshal encodes them.
	payload.ProtoReflect().SetUnknown(buildComboPayloadField204(cursorTsUs, 50))

	msg := &tiktokpb.InboxRequest{
		MessageType:    protoUint64(204),
		SubCommand:     protoUint64(10011),
		ClientVersion:  protoString("1.7.0"),
		Options:        emptyProtoMessage(),
		PlatformFlag:   protoUint64(3),
		Reserved_6:     protoUint64(0),
		GitHash:        protoString("e465244:feat/call-trace-plugin"),
		DeviceId:       protoString(deviceID),
		ClientPlatform: protoString("web"),
		Metadata:       metadataKVsToProto(buildMetadata(deviceID, msToken, verifyFP)),
		FinalFlag:      protoUint64(1),
		Payload:        payload,
	}

	return marshalProto(msg)
}

func mergeInboxConversations(existing, incoming []Conversation) []Conversation {
	indexByID := make(map[string]int, len(existing))
	for i, conv := range existing {
		indexByID[conv.ID] = i
	}

	for _, conv := range incoming {
		if idx, ok := indexByID[conv.ID]; ok {
			if existing[idx].SourceID == 0 {
				existing[idx].SourceID = conv.SourceID
			}
			if existing[idx].Name == "" && conv.Name != "" {
				existing[idx].Name = conv.Name
			}
			if conv.ConversationType != 0 {
				existing[idx].ConversationType = conv.ConversationType
			}
			if conv.Muted != nil {
				existing[idx].Muted = conv.Muted
			}
			if len(conv.Participants) > 0 {
				seenParticipants := make(map[string]struct{}, len(existing[idx].Participants))
				for _, participant := range existing[idx].Participants {
					seenParticipants[participant] = struct{}{}
				}
				for _, participant := range conv.Participants {
					if _, seen := seenParticipants[participant]; seen {
						continue
					}
					existing[idx].Participants = append(existing[idx].Participants, participant)
					seenParticipants[participant] = struct{}{}
				}
			}
			continue
		}

		indexByID[conv.ID] = len(existing)
		existing = append(existing, conv)
	}

	return existing
}

// ---------------------------------------------------------------------------
// Response parser
// ---------------------------------------------------------------------------

// extractPayloadField204 scans the unknown fields of an InboxResponsePayload
// for field 204 (get_by_user_combo response) and returns the InboxConversationList
// inside it. The combo response has two extra wrapper levels vs user_init_list (203):
//
//	field204 → { field1 → { field1=varint(0), field2=entry×N, ... } }
//
// The inner blob (field1's content) has the same layout as InboxConversationList:
// field2 = repeated ConversationDetail (conversations).
func extractPayloadField204(payload *tiktokpb.InboxResponsePayload) *tiktokpb.InboxConversationList {
	if payload == nil {
		return nil
	}
	unknown := payload.ProtoReflect().GetUnknown()
	for len(unknown) > 0 {
		num, typ, n := protowire.ConsumeTag(unknown)
		if n < 0 {
			break
		}
		unknown = unknown[n:]
		if typ == protowire.BytesType {
			val, n2 := protowire.ConsumeBytes(unknown)
			if n2 < 0 {
				break
			}
			unknown = unknown[n2:]
			if num == 204 {
				// val = { field1 → inner }; unwrap field1 to get inner blob.
				inner := unwrapField1Bytes(val)
				if inner == nil {
					return nil
				}
				// inner = { field1=varint(0), field2=ConvDetail×N, ... }
				// This matches InboxConversationList wire layout (field2 = conversations).
				var list tiktokpb.InboxConversationList
				if err := proto.Unmarshal(inner, &list); err == nil {
					return &list
				}
				return nil
			}
		} else if typ == protowire.VarintType {
			_, n2 := protowire.ConsumeVarint(unknown)
			if n2 < 0 {
				return nil
			}
			unknown = unknown[n2:]
		} else {
			return nil
		}
	}
	return nil
}

// unwrapField1Bytes returns the bytes value of the first field-1 (bytes type)
// found in data, or nil if not present.
func unwrapField1Bytes(data []byte) []byte {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil
		}
		data = data[n:]
		if typ == protowire.BytesType {
			val, n2 := protowire.ConsumeBytes(data)
			if n2 < 0 {
				return nil
			}
			if num == 1 {
				return val
			}
			data = data[n2:]
		} else if typ == protowire.VarintType {
			_, n2 := protowire.ConsumeVarint(data)
			if n2 < 0 {
				return nil
			}
			data = data[n2:]
		} else {
			return nil
		}
	}
	return nil
}

// parseInboxResponse decodes a get_by_user_combo response body.
// It returns the conversations and the next-page cursor (0 = no more pages).
// The next-page cursor is the minimum non-zero cursor_ts_us across all returned
// entries, which mirrors how get_by_conversation pagination works.
func parseInboxResponse(ctx context.Context, body []byte) ([]Conversation, uint64, error) {
	var resp tiktokpb.InboxResponse
	if err := unmarshalProto(body, &resp); err != nil {
		return nil, 0, fmt.Errorf("decode top-level response: %w", err)
	}

	// Check server-side status; 0 means success.
	if status := resp.GetStatus(); status != 0 {
		return nil, 0, fmt.Errorf("inbox API returned status %d (%s)", status, resp.GetMessage())
	}

	// Try field-203 (user_init_list) first, then field-204 (user_combo_list).
	// Field 203 may be present with entries but field 204 carries the fuller set
	// of conversations. Prefer whichever has more entries+conversations.
	userInit := resp.GetPayload().GetUserInitList()
	combo := extractPayloadField204(resp.GetPayload())

	field203Total := len(userInit.GetConversations()) + len(userInit.GetEntries())
	field204Total := 0
	if combo != nil {
		field204Total = len(combo.GetConversations()) + len(combo.GetEntries())
	}
	log := zerolog.Ctx(ctx)
	log.Debug().
		Int("field203_total", field203Total).
		Int("field204_total", field204Total).
		Msg("Inbox: comparing field 203 vs field 204 size")

	if field204Total > field203Total {
		log.Debug().
			Int("field203_convs", len(userInit.GetConversations())).
			Int("field203_entries", len(userInit.GetEntries())).
			Int("field204_convs", len(combo.GetConversations())).
			Int("field204_entries", len(combo.GetEntries())).
			Msg("Inbox: switching to field 204 (richer data)")
		userInit = combo
	}
	convs := make([]Conversation, 0, len(userInit.GetConversations())+len(userInit.GetEntries()))
	// TikTok's inbox API returns a fixed set of ~10 conversations with no
	// pagination cursor. nextCursor will always be 0 here on the web API.
	var nextCursor uint64

	details := userInit.GetConversations()
	if len(details) > 0 {
		detailConvs := make([]Conversation, 0, len(details))
		for _, detail := range details {
			// Track minimum last_message_timestamp_us for next-page cursor.
			if ts := detail.GetState().GetLastMessageTimestampUs(); ts != 0 {
				if nextCursor == 0 || ts < nextCursor {
					nextCursor = ts
				}
			}
			conv, err := parseConversationDetailProto(detail)
			if err != nil {
				continue
			}
			detailConvs = append(detailConvs, conv)
		}
		convs = mergeInboxConversations(convs, detailConvs)
	}

	entries := userInit.GetEntries()
	if len(entries) == 0 {
		if len(convs) == 0 {
			return nil, 0, nil // empty inbox
		}
		return convs, 0, nil
	}

	// Also collect cursor from entries (field 25 cursor_ts_us).
	seen := make(map[string]struct{}, len(entries))
	entryConvs := make([]Conversation, 0, len(entries))
	for _, entry := range entries {
		if c := entry.GetCursorTsUs(); c != 0 {
			if nextCursor == 0 || c < nextCursor {
				nextCursor = c
			}
		}
		if !hasRealMessageProto(entry) {
			continue
		}
		conv, err := parseConversationEntryProto(entry)
		if err != nil {
			continue
		}
		if _, dup := seen[conv.ID]; dup {
			continue
		}
		seen[conv.ID] = struct{}{}
		entryConvs = append(entryConvs, conv)
	}
	log.Debug().Int("returning_convs", len(convs)).Msg("Inbox: parseInboxResponse returning")
	return mergeInboxConversations(convs, entryConvs), nextCursor, nil
}

// fetchInboxPage fetches one page of the inbox starting at cursorTsUs (0 = first page).
// Returns conversations on this page and the next-page cursor (0 = no more pages).
func (c *Client) fetchInboxPage(ctx context.Context, deviceID, msToken, verifyFP string, cursorTsUs uint64) ([]Conversation, uint64, error) {
	log := zerolog.Ctx(ctx).With().Str("component", "libtiktok-inbox").Logger()
	ctx = log.WithContext(ctx)
	payload, err := buildInboxPayload(deviceID, msToken, verifyFP, cursorTsUs)
	if err != nil {
		return nil, 0, fmt.Errorf("build inbox payload: %w", err)
	}
	log.Debug().
		Str("device_id", deviceID).
		Uint64("cursor_ts_us", cursorTsUs).
		Int("payload_bytes", len(payload)).
		Msg("Fetching TikTok inbox (get_by_user_combo)")

	resp, err := c.rIA.R().
		SetContext(ctx).
		SetHeader("Accept", "application/x-protobuf").
		SetHeader("Content-Type", "application/x-protobuf").
		SetHeader("Cache-Control", "no-cache").
		SetHeader("Origin", "https://www.tiktok.com").
		SetHeader("Referer", "https://www.tiktok.com/").
		SetQueryParams(map[string]string{
			"aid":             imAID,
			"version_code":    "1.0.0",
			"app_name":        "tiktok_web",
			"device_platform": "web_pc",
			"msToken":         msToken,
			"X-Bogus":         randomBogus(),
		}).
		SetBody(payload).
		Post(inboxURL)
	if err != nil {
		return nil, 0, fmt.Errorf("post inbox: %w", err)
	}
	if resp.IsError() {
		return nil, 0, fmt.Errorf("inbox API returned %d: %s", resp.StatusCode(), resp.String())
	}

	convs, nextCursor, err := parseInboxResponse(ctx, resp.Body())
	if err != nil {
		return nil, 0, fmt.Errorf("parse inbox response: %w", err)
	}
	log.Debug().
		Int("conversations", len(convs)).
		Uint64("next_cursor", nextCursor).
		Msg("Fetched TikTok inbox successfully")
	return convs, nextCursor, nil
}

// buildInboxV2Payload builds the request body for /v2/message/get_by_user_init.
// The envelope is identical to v1 except sub_command=10001 and the pagination
// cursor is an integer offset in field 8 of the payload (field 1 = offset).
// offset=0 fetches the first page; subsequent pages use offset=20 (observed
// in browser captures as the second parallel call).
func buildInboxV2Payload(deviceID, msToken, verifyFP string, offset uint64) ([]byte, error) {
	// Build the inner payload: field 203 → GetUserConversationListRequestBody
	// with cursor = offset (reusing the cursor field as page offset for v2).
	innerPayload := &tiktokpb.InboxRequestPayload{}
	userInitBody := &tiktokpb.GetUserConversationListRequestBody{}
	// field 2 (cursor) = offset for v2 pagination
	if offset > 0 {
		cursorVal := int64(offset)
		userInitBody.Cursor = &cursorVal
	}
	innerPayload.UserInitList = userInitBody

	msg := &tiktokpb.InboxRequest{
		MessageType:    protoUint64(203),
		SubCommand:     protoUint64(10001),
		ClientVersion:  protoString("1.7.0"),
		Options:        emptyProtoMessage(),
		PlatformFlag:   protoUint64(3),
		Reserved_6:     protoUint64(0),
		GitHash:        protoString("e465244:feat/call-trace-plugin"),
		DeviceId:       protoString(deviceID),
		ClientPlatform: protoString("web"),
		Metadata:       metadataKVsToProto(buildMetadata(deviceID, msToken, verifyFP)),
		FinalFlag:      protoUint64(1),
		Payload:        innerPayload,
	}

	return marshalProto(msg)
}

// parseInboxV2Response decodes a /v2/message/get_by_user_init response body.
// The response envelope is InboxResponse; conversations are in
// payload.user_init_list (field 203).
func parseInboxV2Response(ctx context.Context, body []byte) ([]Conversation, error) {
	var resp tiktokpb.InboxResponse
	if err := unmarshalProto(body, &resp); err != nil {
		return nil, fmt.Errorf("decode v2 response: %w", err)
	}
	if status := resp.GetStatus(); status != 0 {
		return nil, fmt.Errorf("inbox v2 API returned status %d (%s)", status, resp.GetMessage())
	}

	userInit := resp.GetPayload().GetUserInitList()
	log := zerolog.Ctx(ctx)
	log.Debug().
		Int("v2_conversations", len(userInit.GetConversations())).
		Int("v2_entries", len(userInit.GetEntries())).
		Msg("Inbox v2: parsed response")

	convs := make([]Conversation, 0, len(userInit.GetConversations())+len(userInit.GetEntries()))

	for _, detail := range userInit.GetConversations() {
		conv, err := parseConversationDetailProto(detail)
		if err != nil {
			continue
		}
		convs = append(convs, conv)
	}

	seen := make(map[string]struct{}, len(convs))
	for _, c := range convs {
		seen[c.ID] = struct{}{}
	}
	for _, entry := range userInit.GetEntries() {
		if !hasRealMessageProto(entry) {
			continue
		}
		conv, err := parseConversationEntryProto(entry)
		if err != nil {
			continue
		}
		if _, dup := seen[conv.ID]; dup {
			continue
		}
		seen[conv.ID] = struct{}{}
		convs = append(convs, conv)
	}
	return convs, nil
}

// fetchInboxV2 fetches one page of conversations from /v2/message/get_by_user_init.
// offset=0 is the first page; offset=20 is the second page (as observed in browser).
func (c *Client) fetchInboxV2(ctx context.Context, deviceID, msToken, verifyFP string, offset uint64) ([]Conversation, error) {
	log := zerolog.Ctx(ctx).With().Str("component", "libtiktok-inbox-v2").Logger()
	ctx = log.WithContext(ctx)
	payload, err := buildInboxV2Payload(deviceID, msToken, verifyFP, offset)
	if err != nil {
		return nil, fmt.Errorf("build inbox v2 payload: %w", err)
	}
	log.Debug().
		Str("device_id", deviceID).
		Uint64("offset", offset).
		Int("payload_bytes", len(payload)).
		Msg("Fetching TikTok inbox v2 (get_by_user_init)")

	resp, err := c.rIA.R().
		SetContext(ctx).
		SetHeader("Accept", "application/x-protobuf").
		SetHeader("Content-Type", "application/x-protobuf").
		SetHeader("Cache-Control", "no-cache").
		SetHeader("Origin", "https://www.tiktok.com").
		SetHeader("Referer", "https://www.tiktok.com/").
		SetQueryParams(map[string]string{
			"aid":             imAID,
			"version_code":    "1.0.0",
			"app_name":        "tiktok_web",
			"device_platform": "web_pc",
			"msToken":         msToken,
			"X-Bogus":         randomBogus(),
		}).
		SetBody(payload).
		Post(inboxV2URL)
	if err != nil {
		return nil, fmt.Errorf("post inbox v2: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("inbox v2 API returned %d: %s", resp.StatusCode(), resp.String())
	}

	convs, err := parseInboxV2Response(ctx, resp.Body())
	if err != nil {
		return nil, fmt.Errorf("parse inbox v2 response: %w", err)
	}
	log.Debug().
		Int("conversations", len(convs)).
		Msg("Fetched TikTok inbox v2 successfully")
	return convs, nil
}

// ---------------------------------------------------------------------------
// GetInbox
// ---------------------------------------------------------------------------

// GetInbox fetches the authenticated user's conversation list from the TikTok
// IM API. It calls /v2/message/get_by_user_init (two pages: offset 0 and 20)
// in parallel with the legacy /v1/message/get_by_user_combo, then merges the
// results so all available conversations are returned.
func (c *Client) GetInbox(ctx context.Context, _ int) ([]Conversation, error) {
	log := zerolog.Ctx(ctx).With().Str("component", "libtiktok-inbox").Logger()
	ctx = log.WithContext(ctx)
	// Extract cookie values we need for the request.
	// rIA already has the full cookie header set at construction time.
	cookie := c.rIA.Header.Get("Cookie")
	universalData, err := c.getMessagesUniversalData()
	if err != nil {
		return nil, fmt.Errorf("failed to get universal data: %w", err)
	}

	appContext, err := universalData.getAppContext()
	if err != nil {
		return nil, fmt.Errorf("failed to get appContext: %w", err)
	}
	deviceID, ok := appContext["wid"].(string)
	if !ok {
		return nil, fmt.Errorf("failed to access wid from appContext")
	}

	msToken := extractCookie(cookie, "msToken")
	verifyFP := extractCookie(cookie, "s_v_web_id")

	// Run v1 combo and v2 page 0 in parallel.
	type result struct {
		convs []Conversation
		err   error
	}
	v1Ch := make(chan result, 1)
	v2p0Ch := make(chan result, 1)
	v2p1Ch := make(chan result, 1)

	go func() {
		convs, _, err := c.fetchInboxPage(ctx, deviceID, msToken, verifyFP, 0)
		v1Ch <- result{convs, err}
	}()
	go func() {
		convs, err := c.fetchInboxV2(ctx, deviceID, msToken, verifyFP, 0)
		v2p0Ch <- result{convs, err}
	}()
	go func() {
		convs, err := c.fetchInboxV2(ctx, deviceID, msToken, verifyFP, 20)
		v2p1Ch <- result{convs, err}
	}()

	v1Res := <-v1Ch
	v2p0Res := <-v2p0Ch
	v2p1Res := <-v2p1Ch

	if v1Res.err != nil {
		log.Warn().Err(v1Res.err).Msg("Inbox v1 failed")
	}
	if v2p0Res.err != nil {
		log.Warn().Err(v2p0Res.err).Msg("Inbox v2 page 0 failed")
	}
	if v2p1Res.err != nil {
		log.Warn().Err(v2p1Res.err).Msg("Inbox v2 page 1 failed")
	}

	// Merge: start with v2 results (richer), then fold in v1.
	var convs []Conversation
	convs = mergeInboxConversations(convs, v2p0Res.convs)
	convs = mergeInboxConversations(convs, v2p1Res.convs)
	convs = mergeInboxConversations(convs, v1Res.convs)

	if len(convs) == 0 && v1Res.err != nil {
		return nil, v1Res.err
	}

	log.Debug().Int("conversations", len(convs)).Msg("Inbox fetched (v1+v2 merged)")
	return convs, nil
}

// ---------------------------------------------------------------------------
// GetMessages
// ---------------------------------------------------------------------------

// buildGetByConversationPayload constructs the type-301 protobuf request body
// for the get_by_conversation endpoint. sourceID is the uint64 from field 5 of
// the conversation entry (Conversation.SourceID); cursorTsUs is the microsecond
// timestamp cursor from field 25 of the last seen message (0 for the first page).
func buildGetByConversationPayload(deviceID, msToken, verifyFP, convID string, sourceID uint64, count int, cursorTsUs uint64) ([]byte, error) {
	msg := &tiktokpb.GetByConversationRequest{
		MessageType:    protoUint64(301),
		SubCommand:     protoUint64(1),
		ClientVersion:  protoString("1.6.0"),
		Options:        emptyProtoMessage(),
		PlatformFlag:   protoUint64(3),
		Reserved_6:     protoUint64(0),
		GitHash:        protoString(""),
		ClientPlatform: protoString("web"),
		Metadata:       metadataKVsToProto(buildMetadata(deviceID, msToken, verifyFP)),
		FinalFlag:      protoUint64(1),
		Payload: &tiktokpb.GetByConversationRequestPayload{
			Query: &tiktokpb.GetByConversationQuery{
				ConversationId: protoString(convID),
				Direction:      protoUint64(1),
				SourceId:       protoUint64(sourceID),
				Reserved_4:     protoUint64(1),
				CursorTsUs:     protoUint64(cursorTsUs),
				Count:          protoUint64(uint64(count)),
			},
		},
	}

	return marshalProto(msg)
}

// parseMessageEntry decodes a single message entry from the response.
// It returns the Message and the raw field-25 timestamp (µs) used as the
// pagination cursor.
func parseMessageEntry(ctx context.Context, c *Client, entry *tiktokpb.ConversationMessageEntry) (Message, uint64, error) {
	cursorTs := entry.GetCursorTsUs()
	if shouldSkipSyncedMessage(entry.GetTags()) {
		return Message{}, cursorTs, errSkipSyncedMessage
	}

	convID := entry.GetConversationId()
	senderID := strconv.FormatUint(entry.GetSenderUserId(), 10)
	tsMicros := entry.GetTimestampUs()
	serverID := entry.GetServerMessageId()
	msgID := extractClientMsgIDFromTags(entry.GetTags())
	contentJSON := entry.GetContentJson()
	zerolog.Ctx(ctx).Debug().
		Uint64("server_message_id", entry.GetServerMessageId()).
		RawJSON("content_json", func() []byte {
			if len(contentJSON) == 0 {
				return []byte(`null`)
			}
			return contentJSON
		}()).
		Msg("parseMessageEntry: raw content_json")
	msgType, text, mediaURL, mimeType := parseMessageContent(ctx, c, contentJSON)
	messageSubtype := entry.GetMessageSubtype()
	thumbURL := ""
	decryptKey := ""
	mediaWidth := 0
	mediaHeight := 0
	mediaDurationMs := 0
	if privateType, assetURL, assetThumbURL, assetDecryptKey, width, height, durationMs, ok := parsePrivateMediaFromConversationEntryProto(entry); ok {
		msgType = privateType
		mediaURL = assetURL
		thumbURL = assetThumbURL
		decryptKey = assetDecryptKey
		mimeType = ""
		mediaWidth = width
		mediaHeight = height
		mediaDurationMs = durationMs
		if text == "" {
			if privateType == "video" {
				text = "[video]"
			} else {
				text = "[photo]"
			}
		}
	}
	if stickerURL, stickerText, stickerMIME, ok := parseStickerFromConversationEntryProto(entry); ok {
		msgType = "sticker"
		text = stickerText
		mediaURL = stickerURL
		thumbURL = ""
		decryptKey = ""
		mimeType = stickerMIME
		mediaWidth = 0
		mediaHeight = 0
	}
	replyTo := uint64(0)
	replyQuoted := ""
	if ref := entry.GetMessageReply(); ref != nil {
		replyTo = ref.GetReferencedServerMessageId()
		replyQuoted = parseReplyQuotedTextFromWire(ref.GetQuotedContextJson())
	}
	rawJSON := append([]byte(nil), contentJSON...)

	return Message{
		ServerID:        serverID,
		ClientMessageID: msgID,
		ConvID:          convID,
		SenderID:        senderID,
		Type:            msgType,
		MessageSubtype:  messageSubtype,
		Text:            text,
		MediaURL:        mediaURL,
		ThumbnailURL:    thumbURL,
		MediaDecryptKey: decryptKey,
		MimeType:        mimeType,
		MediaWidth:      mediaWidth,
		MediaHeight:     mediaHeight,
		MediaDurationMs: mediaDurationMs,
		TimestampMs:     int64(tsMicros) / 1000,
		TimestampUs:     tsMicros,
		Reactions:       parseReactionsProto(entry.GetReactions()),
		ReplyToServerID: replyTo,
		ReplyQuotedText: replyQuoted,
		SendChainID:     entry.GetSendChainId(),
		SenderSecUID:    entry.GetSenderSecUid(),
		CursorTsUs:      entry.GetCursorTsUs(),
		RawContentJSON:  rawJSON,
	}, cursorTs, nil
}

// parseGetByConversationResponse decodes the protobuf response body.
// Returns the list of messages and the next-page cursor: the minimum non-zero
// field-25 cursor_ts_us among entries (oldest anchor in the batch). Using the
// minimum is required regardless of whether the server lists entries newest-
// or oldest-first; using only the last wire row could repeat the same page forever.
func parseGetByConversationResponse(ctx context.Context, c *Client, body []byte) ([]Message, string, error) {
	log := zerolog.Ctx(ctx)
	var resp tiktokpb.GetByConversationResponse
	if err := unmarshalProto(body, &resp); err != nil {
		return nil, "", fmt.Errorf("decode top-level response: %w", err)
	}

	entries := resp.GetPayload().GetConversation().GetEntries()
	if len(entries) == 0 {
		return nil, "", nil
	}

	messages := make([]Message, 0, len(entries))
	var minCursorTs uint64
	considerCursor := func(cursorTs uint64) {
		if cursorTs == 0 {
			return
		}
		if minCursorTs == 0 || cursorTs < minCursorTs {
			minCursorTs = cursorTs
		}
	}
	for i, entry := range entries {
		m, cursorTs, err := parseMessageEntry(ctx, c, entry)
		if errors.Is(err, errSkipSyncedMessage) {
			log.Debug().
				Int("entry_index", i).
				Int("entries_total", len(entries)).
				Uint64("cursor_ts_us", cursorTs).
				Msg("Skipping synced/recalled conversation message entry")
			considerCursor(cursorTs)
			continue
		}
		if err != nil {
			log.Debug().
				Err(err).
				Int("entry_index", i).
				Int("entries_total", len(entries)).
				Msg("Failed to parse conversation message entry")
			continue
		}
		messages = append(messages, m)
		considerCursor(cursorTs)
	}

	// Reverse so messages are in chronological order (oldest first).
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}

	nextCursor := ""
	if minCursorTs != 0 {
		nextCursor = strconv.FormatUint(minCursorTs, 10)
	}
	return messages, nextCursor, nil
}

// GetMessages fetches up to 20 messages for the given conversation.
// Pass an empty cursor for the first page; subsequent pages use the cursor
// string returned by the previous call (the field-25 µs timestamp of the
// oldest message in the last batch).
// Returns the messages and the next-page cursor (empty string when exhausted).
func (c *Client) GetMessages(ctx context.Context, conv *Conversation, cursor string) ([]Message, string, error) {
	log := zerolog.Ctx(ctx).With().Str("component", "libtiktok-messages").Logger()
	ctx = log.WithContext(ctx)
	cookie := c.rIA.Header.Get("Cookie")

	universalData, err := c.getMessagesUniversalData()
	if err != nil {
		return nil, "", fmt.Errorf("failed to get universal data: %w", err)
	}

	appContext, err := universalData.getAppContext()
	if err != nil {
		return nil, "", fmt.Errorf("failed to get appContext: %w", err)
	}
	deviceID, ok := appContext["wid"].(string)
	if !ok {
		return nil, "", fmt.Errorf("failed to access wid from appContext")
	}

	msToken := extractCookie(cookie, "msToken")
	verifyFP := extractCookie(cookie, "s_v_web_id")

	var cursorTsUs uint64
	if cursor != "" {
		cursorTsUs, err = strconv.ParseUint(cursor, 10, 64)
		if err != nil {
			return nil, "", fmt.Errorf("parse cursor %q: %w", cursor, err)
		}
	}

	payload, err := buildGetByConversationPayload(deviceID, msToken, verifyFP, conv.ID, conv.SourceID, 20, cursorTsUs)
	if err != nil {
		return nil, "", fmt.Errorf("build get_by_conversation payload: %w", err)
	}
	log.Debug().
		Str("conversation_id", conv.ID).
		Uint64("source_id", conv.SourceID).
		Uint64("cursor_ts_us", cursorTsUs).
		Int("payload_bytes", len(payload)).
		Msg("Fetching TikTok conversation history")

	resp, err := c.rIA.R().
		SetContext(ctx).
		SetHeader("Accept", "application/x-protobuf").
		SetHeader("Content-Type", "application/x-protobuf").
		SetHeader("Cache-Control", "no-cache").
		SetHeader("Origin", "https://www.tiktok.com").
		SetBody(payload).
		Post(getByConvURL)
	if err != nil {
		return nil, "", fmt.Errorf("post get_by_conversation: %w", err)
	}
	if resp.IsError() {
		return nil, "", fmt.Errorf("get_by_conversation API returned %d: %s", resp.StatusCode(), resp.String())
	}

	messages, nextCursor, err := parseGetByConversationResponse(ctx, c, resp.Body())
	if err != nil {
		return nil, "", fmt.Errorf("parse get_by_conversation response: %w", err)
	}
	log.Debug().
		Str("conversation_id", conv.ID).
		Int("messages", len(messages)).
		Str("next_cursor", nextCursor).
		Msg("Fetched TikTok conversation history successfully")
	return messages, nextCursor, nil
}

// LatestMessageTimestampUs returns ConversationMessageEntry.timestamp_us (wire
// field 4) for the newest message in the thread. It uses get_by_conversation with
// an empty cursor (latest batch); parseGetByConversationResponse orders messages
// oldest-first, so the last row is the newest.
//
// TikTok's mark_read body uses read_message_index with this same field-4 value.
func (c *Client) LatestMessageTimestampUs(ctx context.Context, conv *Conversation) (uint64, error) {
	if conv == nil {
		return 0, nil
	}
	msgs, _, err := c.GetMessages(ctx, conv, "")
	if err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].TimestampUs != 0 {
			return msgs[i].TimestampUs, nil
		}
	}
	return 0, nil
}

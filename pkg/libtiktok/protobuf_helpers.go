package libtiktok

import (
	"fmt"
	"strconv"
	"strings"

	tiktokpb "github.com/httpjamesm/matrix-tiktok/pkg/libtiktok/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// extractVarintField scans unknown protobuf wire bytes for the first occurrence
// of the given field number with varint wire type and returns its value.
// Returns 0 if not found.
func extractVarintField(unknown []byte, fieldNum protowire.Number) uint64 {
	for len(unknown) > 0 {
		num, typ, n := protowire.ConsumeTag(unknown)
		if n < 0 {
			return 0
		}
		unknown = unknown[n:]
		if typ == protowire.VarintType {
			v, n2 := protowire.ConsumeVarint(unknown)
			if n2 < 0 {
				return 0
			}
			if num == fieldNum {
				return v
			}
			unknown = unknown[n2:]
		} else if typ == protowire.BytesType {
			_, n2 := protowire.ConsumeBytes(unknown)
			if n2 < 0 {
				return 0
			}
			unknown = unknown[n2:]
		} else {
			return 0
		}
	}
	return 0
}

func protoBool(v bool) *bool {
	return &v
}

func protoString(v string) *string {
	return &v
}

func protoUint64(v uint64) *uint64 {
	return &v
}

func protoInt32(v int32) *int32 {
	return &v
}

func protoInt64(v int64) *int64 {
	return &v
}

func emptyProtoMessage() *tiktokpb.EmptyMessage {
	return &tiktokpb.EmptyMessage{}
}

func marshalProto(msg proto.Message) ([]byte, error) {
	return proto.Marshal(msg)
}

func unmarshalProto(data []byte, msg proto.Message) error {
	return proto.Unmarshal(data, msg)
}

func metadataKVsToProto(pairs []metaKV) []*tiktokpb.MetadataKV {
	out := make([]*tiktokpb.MetadataKV, 0, len(pairs))
	for _, kv := range pairs {
		out = append(out, &tiktokpb.MetadataKV{
			Key:   protoString(kv.k),
			Value: protoString(kv.v),
		})
	}
	return out
}

func extractClientMsgIDFromTags(tags []*tiktokpb.MetadataTag) string {
	for _, tag := range tags {
		if tag.GetKey() == "s:client_message_id" && len(tag.GetValue()) > 0 {
			return string(tag.GetValue())
		}
	}
	return ""
}

// shouldSkipSyncedMessage reports whether a get_by_conversation row or WS chat
// detail is a recalled or invisible placeholder. TikTok carries these on field 9
// (repeated MetadataTag tags).
func shouldSkipSyncedMessage(tags []*tiktokpb.MetadataTag) bool {
	for _, tag := range tags {
		if tag == nil {
			continue
		}
		switch tag.GetKey() {
		case "s:is_recalled":
			if strings.TrimSpace(string(tag.GetValue())) == "1" {
				return true
			}
		case "s:invisible":
			if strings.TrimSpace(string(tag.GetValue())) != "" {
				return true
			}
		}
	}
	return false
}

func parseReactionsProto(entries []*tiktokpb.ReactionSummary) []Reaction {
	if len(entries) == 0 {
		return nil
	}

	out := make([]Reaction, 0, len(entries))
	for _, entry := range entries {
		emoji := strings.TrimPrefix(entry.GetReactionKey(), "e:")
		if emoji == "" {
			continue
		}

		userEntries := entry.GetUsers().GetEntries()
		userIDs := make([]string, 0, len(userEntries))
		for _, user := range userEntries {
			if uid := user.GetUserIdStr(); uid != "" {
				userIDs = append(userIDs, uid)
			} else if uid := user.GetUserId(); uid != 0 {
				userIDs = append(userIDs, strconv.FormatUint(uid, 10))
			}
		}

		out = append(out, Reaction{Emoji: emoji, UserIDs: userIDs})
	}
	return deduplicateReactions(out)
}

func metadataKVValue(metadata []*tiktokpb.MetadataKV, key string) string {
	for _, pair := range metadata {
		if pair.GetKey() == key {
			return pair.GetValue()
		}
	}
	return ""
}

func extractConversationMuted(attrs []*tiktokpb.MetadataKV) *bool {
	raw := strings.TrimSpace(metadataKVValue(attrs, "a:conv_set_notification"))
	if raw == "" {
		return nil
	}
	muted := raw == "2"
	return &muted
}

func hasRealMessageProto(entry *tiktokpb.InboxConversationEntry) bool {
	raw := entry.GetLastMessagePreview()
	if len(raw) > 0 && !strings.EqualFold(strings.TrimSpace(string(raw)), "placeholder") {
		return true
	}
	if entry.GetSourceId() != 0 {
		return true
	}
	if entry.GetLastServerMessageId() != 0 {
		return true
	}
	if entry.GetLastMessageType() != 0 {
		return true
	}
	return false
}

func parseConversationEntryProto(entry *tiktokpb.InboxConversationEntry) (Conversation, error) {
	convID := entry.GetConversationId()
	sourceID := entry.GetSourceId()
	if convID == "" {
		return Conversation{}, fmt.Errorf("missing conversation ID")
	}

	participants := []string(nil)
	if strings.Contains(convID, ":") {
		parts := strings.Split(convID, ":")
		if len(parts) < 2 {
			return Conversation{}, fmt.Errorf("unexpected convID format: %q", convID)
		}
		participants = parts[len(parts)-2:]
	}

	return Conversation{
		ID:               convID,
		SourceID:         sourceID,
		Participants:     participants,
		ConversationType: entry.GetConversationType(),
	}, nil
}

func parseConversationDetailProto(detail *tiktokpb.InboxConversationDetail) (Conversation, error) {
	convID := detail.GetConversationId()
	if convID == "" {
		return Conversation{}, fmt.Errorf("missing conversation ID")
	}

	sourceID := detail.GetSourceId()
	if sourceID == 0 {
		sourceID = detail.GetCore().GetSourceId()
	}
	// The combo endpoint (get_by_user_combo) encodes conversation_type at wire
	// field 2 and the real source_id at wire field 5. Since field 5 is not in
	// the compiled proto descriptor, it lands in unknown fields — extract it.
	if comboSourceID := extractVarintField(detail.ProtoReflect().GetUnknown(), 5); comboSourceID > 10 {
		sourceID = comboSourceID
	}

	conversationType := detail.GetConversationType()
	if conversationType == 0 {
		conversationType = detail.GetCore().GetConversationType()
	}
	// In the combo response, wire field 2 = conversation_type (1=DM) and
	// wire field 3 = a timestamp. If field 5 (source_id) is present in unknown
	// fields, we know this is a combo entry: use GetSourceId() as the type.
	if extractVarintField(detail.ProtoReflect().GetUnknown(), 5) > 0 {
		if t := detail.GetSourceId(); t > 0 && t <= 10 {
			conversationType = t
		} else if conversationType > 1000000 {
			conversationType = 1
		}
	} else if conversationType > 1000000 {
		conversationType = 1 // looks like timestamp, assume DM
	}
	name := detail.GetCore().GetTitle()

	participants := make([]string, 0, len(detail.GetMembers().GetEntries()))
	for _, member := range detail.GetMembers().GetEntries() {
		if uid := member.GetUserId(); uid != 0 {
			participants = append(participants, strconv.FormatUint(uid, 10))
		}
	}

	if len(participants) == 0 && strings.Contains(convID, ":") {
		parts := strings.Split(convID, ":")
		if len(parts) >= 2 {
			participants = append(participants, parts[len(parts)-2:]...)
		}
	}

	return Conversation{
		ID:               convID,
		SourceID:         sourceID,
		Participants:     participants,
		Name:             name,
		ConversationType: conversationType,
		Muted:            extractConversationMuted(detail.GetState().GetAttributes()),
	}, nil
}

package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal"

	"bytes"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// Message represents a chat message for our client
type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

// Optional privacy filter. Set the WHATSAPP_ALLOWLIST_PATH env var to a file
// containing one bare JID per line (e.g. "919866457501@s.whatsapp.net"); only
// those 1:1 chats — plus all groups (@g.us) — will be persisted to the local
// SQLite. Lines starting with # and inline `# comments` are ignored. With no
// env var set, all chats are stored (upstream default).
var (
	allowListEnabled bool
	allowedJIDs      = map[string]bool{}
)

func loadAllowList() {
	allowedJIDs = map[string]bool{}
	allowListEnabled = false
	path := strings.TrimSpace(os.Getenv("WHATSAPP_ALLOWLIST_PATH"))
	if path == "" {
		fmt.Println("[allowlist] WHATSAPP_ALLOWLIST_PATH not set — storing all chats (upstream default)")
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("[allowlist] Could not read %s: %v — falling back to storing all chats\n", path, err)
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = line[:idx]
		}
		jid := strings.TrimSpace(line)
		if jid == "" {
			continue
		}
		allowedJIDs[jid] = true
	}
	allowListEnabled = true
	fmt.Printf("[allowlist] Loaded %d allowed JIDs from %s\n", len(allowedJIDs), path)
}

// isStorableJID returns true if data for this chat JID is allowed to be persisted
// to the bridge SQLite. Broadcast lists are always dropped. When the allow-list
// is enabled, only @g.us groups and explicitly-listed JIDs pass.
func isStorableJID(jid string) bool {
	if strings.Contains(jid, "@broadcast") {
		return false
	}
	if !allowListEnabled {
		return true
	}
	if strings.HasSuffix(jid, "@g.us") {
		return true
	}
	return allowedJIDs[jid]
}

// Database handler for storing message history
type MessageStore struct {
	db *sql.DB
}

// Initialize message store
func NewMessageStore() (*MessageStore, error) {
	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}

	// Open SQLite database for messages
	db, err := sql.Open("sqlite3", "file:store/messages.db?_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return &MessageStore{db: db}, nil
}

// Close the database connection
func (store *MessageStore) Close() error {
	return store.db.Close()
}

// Store a chat in the database
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	if !isStorableJID(jid) {
		return nil
	}
	_, err := store.db.Exec(
		"INSERT OR REPLACE INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
		jid, name, lastMessageTime,
	)
	return err
}

// UpsertGroup inserts or updates a group's name without touching
// last_message_time. Used by the joined-groups bootstrap/resync paths, which
// know the JID and current name but have no message timestamp to attach. On
// rename, only the name changes; never-active groups keep last_message_time
// NULL so they sort to the bottom of list_chats by default.
func (store *MessageStore) UpsertGroup(jid, name string) error {
	if !isStorableJID(jid) {
		return nil
	}
	_, err := store.db.Exec(
		`INSERT INTO chats (jid, name) VALUES (?, ?)
		 ON CONFLICT(jid) DO UPDATE SET name = excluded.name
		 WHERE COALESCE(chats.name, '') IS NOT COALESCE(excluded.name, '')`,
		jid, name,
	)
	return err
}

// Store a message in the database
func (store *MessageStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	// Only store if there's actual content or media
	if content == "" && mediaType == "" {
		return nil
	}
	if !isStorableJID(chatJID) {
		return nil
	}

	_, err := store.db.Exec(
		`INSERT OR REPLACE INTO messages
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, chatJID, sender, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	)
	return err
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT sender, content, timestamp, is_from_me, media_type, filename FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT ?",
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		var timestamp time.Time
		err := rows.Scan(&msg.Sender, &msg.Content, &timestamp, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		msg.Time = timestamp
		messages = append(messages, msg)
	}

	return messages, nil
}

// Get all chats
func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query("SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}

	return chats, nil
}

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}

	// Try to get text content
	if text := msg.GetConversation(); text != "" {
		return text
	} else if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}

	// For now, we're ignoring non-text messages
	return ""
}

// SendMessageResponse represents the response for the send message API
type SendMessageResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SendMessageRequest represents the request body for the send message API
type SendMessageRequest struct {
	Recipient string `json:"recipient"`
	Message   string `json:"message"`
	MediaPath string `json:"media_path,omitempty"`
	// Mentions is an optional list of participants to @-tag. Each entry is a phone
	// number (country code, no '+') or a full JID. The caller must also include the
	// matching "@<number>" token in Message text so the client renders the tag.
	Mentions []string `json:"mentions,omitempty"`
}

// Function to send a WhatsApp message
func sendWhatsAppMessage(client *whatsmeow.Client, recipient string, message string, mediaPath string, mentions []string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}

	// Create JID for recipient
	var recipientJID types.JID
	var err error

	// Check if recipient is a JID
	isJID := strings.Contains(recipient, "@")

	if isJID {
		// Parse the JID string
		recipientJID, err = types.ParseJID(recipient)
		if err != nil {
			return false, fmt.Sprintf("Error parsing JID: %v", err)
		}
	} else {
		// Create JID from phone number
		recipientJID = types.JID{
			User:   recipient,
			Server: "s.whatsapp.net", // For personal chats
		}
	}

	msg := &waProto.Message{}

	// Check if we have media to send
	if mediaPath != "" {
		// Read media file
		mediaData, err := os.ReadFile(mediaPath)
		if err != nil {
			return false, fmt.Sprintf("Error reading media file: %v", err)
		}

		// Determine media type and mime type based on file extension
		fileExt := strings.ToLower(mediaPath[strings.LastIndex(mediaPath, ".")+1:])
		var mediaType whatsmeow.MediaType
		var mimeType string

		// Handle different media types
		switch fileExt {
		// Image types
		case "jpg", "jpeg":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/jpeg"
		case "png":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/png"
		case "gif":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/gif"
		case "webp":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/webp"

		// Audio types
		case "ogg":
			mediaType = whatsmeow.MediaAudio
			mimeType = "audio/ogg; codecs=opus"

		// Video types
		case "mp4":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/mp4"
		case "avi":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/avi"
		case "mov":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/quicktime"

		// Document types (for any other file type)
		default:
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/octet-stream"
		}

		// Upload media to WhatsApp servers
		resp, err := client.Upload(context.Background(), mediaData, mediaType)
		if err != nil {
			return false, fmt.Sprintf("Error uploading media: %v", err)
		}

		fmt.Println("Media uploaded", resp)

		// Create the appropriate message type based on media type
		switch mediaType {
		case whatsmeow.MediaImage:
			msg.ImageMessage = &waProto.ImageMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaAudio:
			// Handle ogg audio files
			var seconds uint32 = 30 // Default fallback
			var waveform []byte = nil

			// Try to analyze the ogg file
			if strings.Contains(mimeType, "ogg") {
				analyzedSeconds, analyzedWaveform, err := analyzeOggOpus(mediaData)
				if err == nil {
					seconds = analyzedSeconds
					waveform = analyzedWaveform
				} else {
					return false, fmt.Sprintf("Failed to analyze Ogg Opus file: %v", err)
				}
			} else {
				fmt.Printf("Not an Ogg Opus file: %s\n", mimeType)
			}

			msg.AudioMessage = &waProto.AudioMessage{
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
				Seconds:       proto.Uint32(seconds),
				PTT:           proto.Bool(true),
				Waveform:      waveform,
			}
		case whatsmeow.MediaVideo:
			msg.VideoMessage = &waProto.VideoMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaDocument:
			msg.DocumentMessage = &waProto.DocumentMessage{
				Title:         proto.String(mediaPath[strings.LastIndex(mediaPath, "/")+1:]),
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		}
	} else if len(mentions) > 0 {
		// Build an ExtendedTextMessage so we can attach mentioned JIDs. WhatsApp
		// renders an "@<number>" token in the text as the contact's name only when
		// the corresponding JID is present in ContextInfo.MentionedJID.
		mentionedJIDs := make([]string, 0, len(mentions))
		for _, m := range mentions {
			m = strings.TrimSpace(m)
			if m == "" {
				continue
			}
			if strings.Contains(m, "@") {
				mentionedJIDs = append(mentionedJIDs, m) // already a JID (e.g. ...@s.whatsapp.net or ...@lid)
			} else {
				mentionedJIDs = append(mentionedJIDs, m+"@s.whatsapp.net")
			}
		}
		msg.ExtendedTextMessage = &waProto.ExtendedTextMessage{
			Text: proto.String(message),
			ContextInfo: &waProto.ContextInfo{
				MentionedJID: mentionedJIDs,
			},
		}
	} else {
		msg.Conversation = proto.String(message)
	}

	// Send message
	_, err = client.SendMessage(context.Background(), recipientJID, msg)

	if err != nil {
		return false, fmt.Sprintf("Error sending message: %v", err)
	}

	return true, fmt.Sprintf("Message sent to %s", recipient)
}

// Extract media info from a message
func extractMediaInfo(msg *waProto.Message) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}

	// Check for image message
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + time.Now().Format("20060102_150405") + ".jpg",
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	// Check for video message
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + time.Now().Format("20060102_150405") + ".mp4",
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	// Check for audio message
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + time.Now().Format("20060102_150405") + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	// Check for document message
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + time.Now().Format("20060102_150405")
		}
		return "document", filename,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", nil, nil, nil, 0
}

// Handle regular incoming messages with media support
func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, logger waLog.Logger) {
	// Save message to database
	chatJID := msg.Info.Chat.String()
	sender := msg.Info.Sender.User

	// Get appropriate chat name (pass nil for conversation since we don't have one for regular messages)
	name := GetChatName(client, messageStore, msg.Info.Chat, chatJID, nil, sender, logger)

	// Update chat in database with the message timestamp (keeps last message time updated)
	err := messageStore.StoreChat(chatJID, name, msg.Info.Timestamp)
	if err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}

	// Extract text content
	content := extractTextContent(msg.Message)

	// Extract media info
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message)

	// Skip if there's no content and no media
	if content == "" && mediaType == "" {
		return
	}

	// Store message in database
	err = messageStore.StoreMessage(
		msg.Info.ID,
		chatJID,
		sender,
		content,
		msg.Info.Timestamp,
		msg.Info.IsFromMe,
		mediaType,
		filename,
		url,
		mediaKey,
		fileSHA256,
		fileEncSHA256,
		fileLength,
	)

	if err != nil {
		logger.Warnf("Failed to store message: %v", err)
	} else {
		// Log message reception
		timestamp := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
		direction := "←"
		if msg.Info.IsFromMe {
			direction = "→"
		}

		// Log based on message type
		if mediaType != "" {
			fmt.Printf("[%s] %s %s: [%s: %s] %s\n", timestamp, direction, sender, mediaType, filename, content)
		} else if content != "" {
			fmt.Printf("[%s] %s %s: %s\n", timestamp, direction, sender, content)
		}
	}
}

// DownloadMediaRequest represents the request body for the download media API
type DownloadMediaRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
}

// DownloadMediaResponse represents the response for the download media API
type DownloadMediaResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	Filename string `json:"filename,omitempty"`
	Path     string `json:"path,omitempty"`
}

// CreateGroupRequest represents the request body for the create group API
type CreateGroupRequest struct {
	Name                 string   `json:"name"`
	Participants         []string `json:"participants"`
	Announce             bool     `json:"announce,omitempty"`
	Locked               bool     `json:"locked,omitempty"`
	EphemeralSeconds     uint32   `json:"ephemeral_seconds,omitempty"`
	IsCommunity          bool     `json:"is_community,omitempty"`
	CommunityParentJID   string   `json:"community_parent_jid,omitempty"`
	JoinApprovalRequired bool     `json:"join_approval_required,omitempty"`
}

// CreateGroupResponse represents the response for the create group API
type CreateGroupResponse struct {
	Success          bool     `json:"success"`
	Message          string   `json:"message"`
	JID              string   `json:"jid,omitempty"`
	Name             string   `json:"name,omitempty"`
	ParticipantCount int      `json:"participant_count,omitempty"`
	FailedAdds       []string `json:"failed_adds,omitempty"`
}

// LeaveGroupRequest represents the request body for the leave group API
type LeaveGroupRequest struct {
	JID string `json:"jid"`
}

// LeaveGroupResponse represents the response for the leave group API
type LeaveGroupResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// AddGroupParticipantsRequest represents the request body for the add participants API.
type AddGroupParticipantsRequest struct {
	GroupJID     string   `json:"group_jid"`
	Participants []string `json:"participants"`
}

// ParticipantResult is one row of the add-participants outcome.
type ParticipantResult struct {
	JID         string `json:"jid"`
	ErrorCode   int    `json:"error_code,omitempty"`
	InviteSent  bool   `json:"invite_sent,omitempty"`
	InviteError string `json:"invite_error,omitempty"`
}

// GetGroupInviteLinkRequest represents the request body for the invite-link API.
type GetGroupInviteLinkRequest struct {
	JID   string `json:"jid"`
	Reset bool   `json:"reset,omitempty"`
}

// GetGroupInviteLinkResponse represents the response for the invite-link API.
type GetGroupInviteLinkResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	JID     string `json:"jid,omitempty"`
	Link    string `json:"link,omitempty"`
}

// getGroupInviteLink fetches (or, when reset=true, regenerates) a group's invite link.
func getGroupInviteLink(client *whatsmeow.Client, jidStr string, reset bool) GetGroupInviteLinkResponse {
	if !client.IsConnected() {
		return GetGroupInviteLinkResponse{Success: false, Message: "Not connected to WhatsApp"}
	}
	groupJID, err := parseGroupJID(jidStr)
	if err != nil {
		return GetGroupInviteLinkResponse{Success: false, Message: err.Error()}
	}
	link, err := client.GetGroupInviteLink(context.Background(), groupJID, reset)
	if err != nil {
		return GetGroupInviteLinkResponse{Success: false, Message: fmt.Sprintf("Error fetching invite link: %v", err)}
	}
	return GetGroupInviteLinkResponse{Success: true, JID: groupJID.String(), Link: link}
}

// AddGroupParticipantsResponse represents the response for the add participants API.
type AddGroupParticipantsResponse struct {
	Success bool                `json:"success"`
	Message string              `json:"message"`
	Added   []ParticipantResult `json:"added,omitempty"`
	Failed  []ParticipantResult `json:"failed,omitempty"`
}

// GetGroupInfoRequest represents the request body for the get group info API.
type GetGroupInfoRequest struct {
	JID string `json:"jid"`
}

// GroupParticipantInfo is the JSON shape for a single group member.
type GroupParticipantInfo struct {
	JID          string `json:"jid"`
	PhoneNumber  string `json:"phone_number,omitempty"`
	LID          string `json:"lid,omitempty"`
	IsAdmin      bool   `json:"is_admin"`
	IsSuperAdmin bool   `json:"is_super_admin"`
	DisplayName  string `json:"display_name,omitempty"`
}

// GetGroupInfoResponse represents the response for the get group info API.
type GetGroupInfoResponse struct {
	Success              bool                   `json:"success"`
	Message              string                 `json:"message,omitempty"`
	JID                  string                 `json:"jid,omitempty"`
	Name                 string                 `json:"name,omitempty"`
	Topic                string                 `json:"topic,omitempty"`
	OwnerJID             string                 `json:"owner_jid,omitempty"`
	Created              string                 `json:"created,omitempty"`
	IsAnnounce           bool                   `json:"is_announce,omitempty"`
	IsLocked             bool                   `json:"is_locked,omitempty"`
	IsEphemeral          bool                   `json:"is_ephemeral,omitempty"`
	DisappearingTimer    uint32                 `json:"disappearing_timer,omitempty"`
	IsCommunity          bool                   `json:"is_community,omitempty"`
	LinkedParentJID      string                 `json:"linked_parent_jid,omitempty"`
	JoinApprovalRequired bool                   `json:"join_approval_required,omitempty"`
	ParticipantCount     int                    `json:"participant_count"`
	Participants         []GroupParticipantInfo `json:"participants"`
}

// ResolveInviteLinkRequest represents the request body for the resolve invite-link API.
type ResolveInviteLinkRequest struct {
	Link string `json:"link"`
}

// ListedGroupInfo is the JSON shape for a single group in the joined-groups list.
type ListedGroupInfo struct {
	JID              string `json:"jid"`
	Name             string `json:"name"`
	Topic            string `json:"topic,omitempty"`
	ParticipantCount int    `json:"participant_count"`
	IsAnnounce       bool   `json:"is_announce,omitempty"`
	IsLocked         bool   `json:"is_locked,omitempty"`
	IsCommunity      bool   `json:"is_community,omitempty"`
}

// ListJoinedGroupsResponse represents the response for the list joined groups API.
type ListJoinedGroupsResponse struct {
	Success bool              `json:"success"`
	Message string            `json:"message,omitempty"`
	Count   int               `json:"count"`
	Groups  []ListedGroupInfo `json:"groups"`
	Synced  int               `json:"synced,omitempty"`
}

// parseGroupJID parses a string into a group JID and validates the @g.us suffix.
func parseGroupJID(jidStr string) (types.JID, error) {
	jidStr = strings.TrimSpace(jidStr)
	if jidStr == "" {
		return types.JID{}, fmt.Errorf("group JID is required")
	}
	jid, err := types.ParseJID(jidStr)
	if err != nil {
		return types.JID{}, fmt.Errorf("invalid JID: %v", err)
	}
	if jid.Server != "g.us" {
		return types.JID{}, fmt.Errorf("only group JIDs (@g.us) are accepted")
	}
	return jid, nil
}

// addGroupParticipants adds the given participants to a group via whatsmeow.
// When WhatsApp refuses to add someone (typically error 403 due to their privacy
// setting "Who can add me to groups"), the bridge automatically fetches the group's
// invite link and DMs it to that contact along with the group name, so they can
// join voluntarily. Each failure entry reports whether the invite link DM succeeded.
func addGroupParticipants(client *whatsmeow.Client, req AddGroupParticipantsRequest) AddGroupParticipantsResponse {
	if !client.IsConnected() {
		return AddGroupParticipantsResponse{Success: false, Message: "Not connected to WhatsApp"}
	}
	groupJID, err := parseGroupJID(req.GroupJID)
	if err != nil {
		return AddGroupParticipantsResponse{Success: false, Message: err.Error()}
	}
	if len(req.Participants) == 0 {
		return AddGroupParticipantsResponse{Success: false, Message: "At least one participant is required"}
	}

	type inputEntry struct {
		original string
		jid      types.JID
	}
	var inputs []inputEntry
	for _, p := range req.Participants {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var pjid types.JID
		var perr error
		if strings.Contains(p, "@") {
			pjid, perr = types.ParseJID(p)
			if perr != nil {
				return AddGroupParticipantsResponse{Success: false, Message: fmt.Sprintf("Invalid participant JID %q: %v", p, perr)}
			}
		} else {
			pjid = types.JID{User: strings.TrimPrefix(p, "+"), Server: "s.whatsapp.net"}
		}
		inputs = append(inputs, inputEntry{original: p, jid: pjid})
	}
	if len(inputs) == 0 {
		return AddGroupParticipantsResponse{Success: false, Message: "No valid participants after parsing"}
	}

	participantJIDs := make([]types.JID, len(inputs))
	for i, e := range inputs {
		participantJIDs[i] = e.jid
	}

	results, err := client.UpdateGroupParticipants(context.Background(), groupJID, participantJIDs, whatsmeow.ParticipantChangeAdd)
	if err != nil {
		return AddGroupParticipantsResponse{Success: false, Message: fmt.Sprintf("Error adding participants: %v", err)}
	}

	// Lazy-fetch the invite link + group name only if at least one failure happens.
	var (
		inviteLink      string
		groupName       string
		inviteFetchErr  error
		inviteAttempted bool
	)
	fetchInvite := func() {
		if inviteAttempted {
			return
		}
		inviteAttempted = true
		link, ferr := client.GetGroupInviteLink(context.Background(), groupJID, false)
		if ferr != nil {
			inviteFetchErr = ferr
			return
		}
		inviteLink = link
		if info, ierr := client.GetGroupInfo(context.Background(), groupJID); ierr == nil {
			groupName = info.Name
		}
	}

	var added, failed []ParticipantResult
	for i, p := range results {
		entry := ParticipantResult{JID: p.JID.String(), ErrorCode: p.Error}
		if p.Error == 0 {
			added = append(added, entry)
			continue
		}

		fetchInvite()
		if inviteFetchErr != nil {
			entry.InviteError = fmt.Sprintf("could not fetch invite link: %v", inviteFetchErr)
			failed = append(failed, entry)
			continue
		}

		// Use the original input (always s.whatsapp.net) as the DM recipient.
		// Falls back to the result's PhoneNumber/JID when ordering can't be relied on.
		var recipient string
		if i < len(inputs) {
			recipient = inputs[i].jid.String()
		} else if !p.PhoneNumber.IsEmpty() {
			recipient = p.PhoneNumber.String()
		} else {
			recipient = p.JID.String()
		}

		var body string
		if groupName != "" {
			body = fmt.Sprintf("You've been invited to join the WhatsApp group \"%s\".\n\n%s\n\nTap the link to join.", groupName, inviteLink)
		} else {
			body = fmt.Sprintf("You've been invited to join a WhatsApp group.\n\n%s\n\nTap the link to join.", inviteLink)
		}

		ok, statusMsg := sendWhatsAppMessage(client, recipient, body, "", nil)
		if ok {
			entry.InviteSent = true
		} else {
			entry.InviteError = statusMsg
		}
		failed = append(failed, entry)
	}

	sentCount := 0
	for _, f := range failed {
		if f.InviteSent {
			sentCount++
		}
	}

	msg := fmt.Sprintf("Added %d participant(s)", len(added))
	if len(failed) > 0 {
		msg += fmt.Sprintf(", %d failed", len(failed))
		if sentCount > 0 {
			msg += fmt.Sprintf(" (sent invite link to %d)", sentCount)
		}
	}
	return AddGroupParticipantsResponse{
		Success: len(added) > 0 || len(failed) == 0,
		Message: msg,
		Added:   added,
		Failed:  failed,
	}
}

// removeGroupParticipants removes the given participants from a group via whatsmeow.
func removeGroupParticipants(client *whatsmeow.Client, req AddGroupParticipantsRequest) AddGroupParticipantsResponse {
	if !client.IsConnected() {
		return AddGroupParticipantsResponse{Success: false, Message: "Not connected to WhatsApp"}
	}
	groupJID, err := parseGroupJID(req.GroupJID)
	if err != nil {
		return AddGroupParticipantsResponse{Success: false, Message: err.Error()}
	}
	if len(req.Participants) == 0 {
		return AddGroupParticipantsResponse{Success: false, Message: "At least one participant is required"}
	}

	var participantJIDs []types.JID
	for _, p := range req.Participants {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var pjid types.JID
		if strings.Contains(p, "@") {
			pjid, err = types.ParseJID(p)
			if err != nil {
				return AddGroupParticipantsResponse{Success: false, Message: fmt.Sprintf("Invalid participant JID %q: %v", p, err)}
			}
		} else {
			pjid = types.JID{User: strings.TrimPrefix(p, "+"), Server: "s.whatsapp.net"}
		}
		participantJIDs = append(participantJIDs, pjid)
	}
	if len(participantJIDs) == 0 {
		return AddGroupParticipantsResponse{Success: false, Message: "No valid participants after parsing"}
	}

	results, err := client.UpdateGroupParticipants(context.Background(), groupJID, participantJIDs, whatsmeow.ParticipantChangeRemove)
	if err != nil {
		return AddGroupParticipantsResponse{Success: false, Message: fmt.Sprintf("Error removing participants: %v", err)}
	}

	var removed, failed []ParticipantResult
	for _, p := range results {
		entry := ParticipantResult{JID: p.JID.String(), ErrorCode: p.Error}
		if p.Error == 0 {
			removed = append(removed, entry)
		} else {
			failed = append(failed, entry)
		}
	}

	msg := fmt.Sprintf("Removed %d participant(s)", len(removed))
	if len(failed) > 0 {
		msg += fmt.Sprintf(", %d failed", len(failed))
	}
	return AddGroupParticipantsResponse{
		Success: len(removed) > 0 || len(failed) == 0,
		Message: msg,
		Added:   removed,
		Failed:  failed,
	}
}

// getGroupInfo returns full group metadata including the participant list.
func getGroupInfo(client *whatsmeow.Client, jidStr string) GetGroupInfoResponse {
	if !client.IsConnected() {
		return GetGroupInfoResponse{Success: false, Message: "Not connected to WhatsApp"}
	}
	groupJID, err := parseGroupJID(jidStr)
	if err != nil {
		return GetGroupInfoResponse{Success: false, Message: err.Error()}
	}

	info, err := client.GetGroupInfo(context.Background(), groupJID)
	if err != nil {
		return GetGroupInfoResponse{Success: false, Message: fmt.Sprintf("Error fetching group info: %v", err)}
	}

	participants := make([]GroupParticipantInfo, 0, len(info.Participants))
	for _, p := range info.Participants {
		pi := GroupParticipantInfo{
			JID:          p.JID.String(),
			IsAdmin:      p.IsAdmin,
			IsSuperAdmin: p.IsSuperAdmin,
			DisplayName:  p.DisplayName,
		}
		if !p.PhoneNumber.IsEmpty() {
			pi.PhoneNumber = p.PhoneNumber.User
		} else if p.JID.Server == "s.whatsapp.net" {
			pi.PhoneNumber = p.JID.User
		}
		if !p.LID.IsEmpty() {
			pi.LID = p.LID.String()
		}
		// Try to enrich with the contact's stored full name when whatsmeow didn't supply one.
		if pi.DisplayName == "" {
			if contact, cerr := client.Store.Contacts.GetContact(context.Background(), p.JID); cerr == nil {
				if contact.FullName != "" {
					pi.DisplayName = contact.FullName
				} else if contact.PushName != "" {
					pi.DisplayName = contact.PushName
				}
			}
		}
		participants = append(participants, pi)
	}

	created := ""
	if !info.GroupCreated.IsZero() {
		created = info.GroupCreated.UTC().Format(time.RFC3339)
	}

	return GetGroupInfoResponse{
		Success:              true,
		JID:                  info.JID.String(),
		Name:                 info.Name,
		Topic:                info.Topic,
		OwnerJID:             info.OwnerJID.String(),
		Created:              created,
		IsAnnounce:           info.IsAnnounce,
		IsLocked:             info.IsLocked,
		IsEphemeral:          info.IsEphemeral,
		DisappearingTimer:    info.DisappearingTimer,
		IsCommunity:          info.IsParent,
		LinkedParentJID:      info.LinkedParentJID.String(),
		JoinApprovalRequired: info.IsJoinApprovalRequired,
		ParticipantCount:     len(info.Participants),
		Participants:         participants,
	}
}

// syncJoinedGroups fetches the full list of groups the account belongs to from
// WhatsApp and upserts each into the local chats table. This is the core fix
// for the "quiet groups are invisible to the MCP" class of bug: the upstream
// bridge only ever inserts rows in response to message events, so any group
// that hasn't seen a message since pairing never appears in list_chats. This
// helper runs on every events.Connected and from a periodic ticker, so renames
// and newly-joined groups also propagate without a restart.
//
// Returns the number of groups upserted and any error.
func syncJoinedGroups(client *whatsmeow.Client, store *MessageStore, logger waLog.Logger) (int, error) {
	if !client.IsConnected() {
		return 0, fmt.Errorf("not connected to WhatsApp")
	}
	groups, err := client.GetJoinedGroups(context.Background())
	if err != nil {
		return 0, fmt.Errorf("error fetching joined groups: %v", err)
	}
	count := 0
	for _, g := range groups {
		if err := store.UpsertGroup(g.JID.String(), g.Name); err != nil {
			logger.Warnf("Failed to upsert group %s: %v", g.JID.String(), err)
			continue
		}
		count++
	}
	return count, nil
}

// listJoinedGroups returns the full live list of groups the account is in,
// straight from WhatsApp (bypassing the local DB). Also upserts each row into
// the local chats table as a side effect, so subsequent list_chats calls find
// them. Useful as an escape hatch when the local DB has drifted out of sync.
func listJoinedGroups(client *whatsmeow.Client, store *MessageStore) ListJoinedGroupsResponse {
	if !client.IsConnected() {
		return ListJoinedGroupsResponse{Success: false, Message: "Not connected to WhatsApp"}
	}
	groups, err := client.GetJoinedGroups(context.Background())
	if err != nil {
		return ListJoinedGroupsResponse{Success: false, Message: fmt.Sprintf("Error fetching joined groups: %v", err)}
	}
	out := make([]ListedGroupInfo, 0, len(groups))
	synced := 0
	for _, g := range groups {
		out = append(out, ListedGroupInfo{
			JID:              g.JID.String(),
			Name:             g.Name,
			Topic:            g.Topic,
			ParticipantCount: len(g.Participants),
			IsAnnounce:       g.IsAnnounce,
			IsLocked:         g.IsLocked,
			IsCommunity:      g.IsParent,
		})
		if store != nil {
			if uerr := store.UpsertGroup(g.JID.String(), g.Name); uerr == nil {
				synced++
			}
		}
	}
	return ListJoinedGroupsResponse{
		Success: true,
		Count:   len(out),
		Groups:  out,
		Synced:  synced,
	}
}

// resolveInviteLink takes a https://chat.whatsapp.com/<code> invite link (or
// a bare invite code) and asks WhatsApp's servers to resolve it to a full
// GroupInfo. On success the result is also upserted into the local chats
// table, so a subsequent send_message / list_chats finds the group without
// any extra hop. This is the single tool that would have unblocked the
// "I have the invite link but the bridge has never seen this group" case.
func resolveInviteLink(client *whatsmeow.Client, store *MessageStore, link string) GetGroupInfoResponse {
	if !client.IsConnected() {
		return GetGroupInfoResponse{Success: false, Message: "Not connected to WhatsApp"}
	}
	code := strings.TrimSpace(link)
	if code == "" {
		return GetGroupInfoResponse{Success: false, Message: "Invite link or code is required"}
	}
	// Accept either a full URL ("https://chat.whatsapp.com/<code>") or the
	// bare code. Strip any trailing query string.
	if idx := strings.LastIndex(code, "/"); idx >= 0 {
		code = code[idx+1:]
	}
	if i := strings.Index(code, "?"); i >= 0 {
		code = code[:i]
	}
	if code == "" {
		return GetGroupInfoResponse{Success: false, Message: "Could not extract invite code from input"}
	}

	info, err := client.GetGroupInfoFromLink(context.Background(), code)
	if err != nil {
		return GetGroupInfoResponse{Success: false, Message: fmt.Sprintf("Error resolving invite link: %v", err)}
	}
	if store != nil {
		if uerr := store.UpsertGroup(info.JID.String(), info.Name); uerr != nil {
			// Non-fatal: caller still gets the resolved JID, just not auto-persisted.
			fmt.Printf("[resolve_invite_link] Failed to upsert %s: %v\n", info.JID.String(), uerr)
		}
	}

	participants := make([]GroupParticipantInfo, 0, len(info.Participants))
	for _, p := range info.Participants {
		pi := GroupParticipantInfo{
			JID:          p.JID.String(),
			IsAdmin:      p.IsAdmin,
			IsSuperAdmin: p.IsSuperAdmin,
			DisplayName:  p.DisplayName,
		}
		if !p.PhoneNumber.IsEmpty() {
			pi.PhoneNumber = p.PhoneNumber.User
		} else if p.JID.Server == "s.whatsapp.net" {
			pi.PhoneNumber = p.JID.User
		}
		if !p.LID.IsEmpty() {
			pi.LID = p.LID.String()
		}
		participants = append(participants, pi)
	}

	created := ""
	if !info.GroupCreated.IsZero() {
		created = info.GroupCreated.UTC().Format(time.RFC3339)
	}

	return GetGroupInfoResponse{
		Success:              true,
		JID:                  info.JID.String(),
		Name:                 info.Name,
		Topic:                info.Topic,
		OwnerJID:             info.OwnerJID.String(),
		Created:              created,
		IsAnnounce:           info.IsAnnounce,
		IsLocked:             info.IsLocked,
		IsEphemeral:          info.IsEphemeral,
		DisappearingTimer:    info.DisappearingTimer,
		IsCommunity:          info.IsParent,
		LinkedParentJID:      info.LinkedParentJID.String(),
		JoinApprovalRequired: info.IsJoinApprovalRequired,
		ParticipantCount:     len(info.Participants),
		Participants:         participants,
	}
}

// getResyncInterval reads the WHATSAPP_GROUP_RESYNC_INTERVAL env var (e.g.
// "10m", "30s"). Defaults to 10 minutes. Values below 30s are rejected as
// likely-typos and fall back to the default.
func getResyncInterval() time.Duration {
	const defaultInterval = 10 * time.Minute
	s := strings.TrimSpace(os.Getenv("WHATSAPP_GROUP_RESYNC_INTERVAL"))
	if s == "" {
		return defaultInterval
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		fmt.Printf("[group-resync] Invalid WHATSAPP_GROUP_RESYNC_INTERVAL=%q (using default %s): %v\n", s, defaultInterval, err)
		return defaultInterval
	}
	if d < 30*time.Second {
		fmt.Printf("[group-resync] WHATSAPP_GROUP_RESYNC_INTERVAL=%s too low, using default %s\n", d, defaultInterval)
		return defaultInterval
	}
	return d
}

// runGroupResyncLoop runs a background ticker that re-syncs joined groups
// from WhatsApp into the local DB. Catches renames, newly-joined groups, and
// any drift that happens while the bridge is running (separate from the
// on-connect bootstrap, which catches drift accumulated while it was down).
func runGroupResyncLoop(client *whatsmeow.Client, store *MessageStore, logger waLog.Logger) {
	interval := getResyncInterval()
	fmt.Printf("[group-resync] Polling joined groups every %s\n", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if !client.IsConnected() {
			continue
		}
		count, err := syncJoinedGroups(client, store, logger)
		if err != nil {
			logger.Warnf("[group-resync] Sync failed: %v", err)
			continue
		}
		logger.Infof("[group-resync] Synced %d groups", count)
	}
}

// leaveWhatsAppGroup leaves the specified group on WhatsApp.
func leaveWhatsAppGroup(client *whatsmeow.Client, jidStr string) LeaveGroupResponse {
	if !client.IsConnected() {
		return LeaveGroupResponse{Success: false, Message: "Not connected to WhatsApp"}
	}
	jidStr = strings.TrimSpace(jidStr)
	if jidStr == "" {
		return LeaveGroupResponse{Success: false, Message: "Group JID is required"}
	}
	jid, err := types.ParseJID(jidStr)
	if err != nil {
		return LeaveGroupResponse{Success: false, Message: fmt.Sprintf("Invalid JID: %v", err)}
	}
	if jid.Server != "g.us" {
		return LeaveGroupResponse{Success: false, Message: "Only group JIDs (@g.us) can be left"}
	}
	if err := client.LeaveGroup(context.Background(), jid); err != nil {
		return LeaveGroupResponse{Success: false, Message: fmt.Sprintf("Error leaving group: %v", err)}
	}
	return LeaveGroupResponse{Success: true, Message: fmt.Sprintf("Left group %s", jid.String())}
}

// createWhatsAppGroup creates a new group on WhatsApp.
func createWhatsAppGroup(client *whatsmeow.Client, messageStore *MessageStore, req CreateGroupRequest) CreateGroupResponse {
	if !client.IsConnected() {
		return CreateGroupResponse{Success: false, Message: "Not connected to WhatsApp"}
	}
	if strings.TrimSpace(req.Name) == "" {
		return CreateGroupResponse{Success: false, Message: "Group name is required"}
	}
	// WhatsApp's real limit is ~100 characters (the historical 25-char comment is outdated; existing
	// groups created via the WhatsApp app already exceed 30 chars).
	if len([]rune(req.Name)) > 100 {
		return CreateGroupResponse{Success: false, Message: "Group name must be 100 characters or fewer"}
	}
	if len(req.Participants) == 0 {
		return CreateGroupResponse{Success: false, Message: "At least one participant is required"}
	}

	participantJIDs := make([]types.JID, 0, len(req.Participants))
	for _, p := range req.Participants {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var jid types.JID
		var err error
		if strings.Contains(p, "@") {
			jid, err = types.ParseJID(p)
			if err != nil {
				return CreateGroupResponse{Success: false, Message: fmt.Sprintf("Invalid participant JID %q: %v", p, err)}
			}
		} else {
			jid = types.JID{User: strings.TrimPrefix(p, "+"), Server: "s.whatsapp.net"}
		}
		participantJIDs = append(participantJIDs, jid)
	}
	if len(participantJIDs) == 0 {
		return CreateGroupResponse{Success: false, Message: "No valid participants after parsing"}
	}

	createReq := whatsmeow.ReqCreateGroup{
		Name:         req.Name,
		Participants: participantJIDs,
	}
	if req.Locked {
		createReq.GroupLocked.IsLocked = true
	}
	if req.Announce {
		createReq.GroupAnnounce.IsAnnounce = true
	}
	if req.EphemeralSeconds > 0 {
		createReq.GroupEphemeral.IsEphemeral = true
		createReq.GroupEphemeral.DisappearingTimer = req.EphemeralSeconds
	}
	if req.JoinApprovalRequired {
		createReq.GroupMembershipApprovalMode.IsJoinApprovalRequired = true
	}
	if req.IsCommunity {
		createReq.GroupParent.IsParent = true
	}
	if req.CommunityParentJID != "" {
		parentJID, err := types.ParseJID(req.CommunityParentJID)
		if err != nil {
			return CreateGroupResponse{Success: false, Message: fmt.Sprintf("Invalid community_parent_jid: %v", err)}
		}
		createReq.GroupLinkedParent.LinkedParentJID = parentJID
	}

	groupInfo, err := client.CreateGroup(context.Background(), createReq)
	if err != nil {
		return CreateGroupResponse{Success: false, Message: fmt.Sprintf("Error creating group: %v", err)}
	}

	// Persist the new chat so it shows up in list_chats immediately.
	createdAt := groupInfo.GroupCreated
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	if err := messageStore.StoreChat(groupInfo.JID.String(), groupInfo.Name, createdAt); err != nil {
		fmt.Printf("Warning: failed to store newly created group chat: %v\n", err)
	}

	// Surface any participants the server refused to add.
	var failed []string
	for _, p := range groupInfo.Participants {
		if p.Error != 0 {
			failed = append(failed, fmt.Sprintf("%s (code %d)", p.JID.String(), p.Error))
		}
	}

	return CreateGroupResponse{
		Success:          true,
		Message:          "Group created",
		JID:              groupInfo.JID.String(),
		Name:             groupInfo.Name,
		ParticipantCount: groupInfo.ParticipantCount,
		FailedAdds:       failed,
	}
}

// --- Extension: rename group, set group photo, promote/demote admins ---

type SetGroupNameRequest struct {
	JID  string `json:"jid"`
	Name string `json:"name"`
}

type SetGroupNameResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	JID     string `json:"jid,omitempty"`
	Name    string `json:"name,omitempty"`
}

func setGroupName(client *whatsmeow.Client, jidStr, name string) SetGroupNameResponse {
	if !client.IsConnected() {
		return SetGroupNameResponse{Success: false, Message: "Not connected to WhatsApp"}
	}
	jid, err := parseGroupJID(jidStr)
	if err != nil {
		return SetGroupNameResponse{Success: false, Message: err.Error()}
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return SetGroupNameResponse{Success: false, Message: "Name is required"}
	}
	if len([]rune(name)) > 100 {
		return SetGroupNameResponse{Success: false, Message: "Group name must be 100 characters or fewer"}
	}
	if err := client.SetGroupName(context.Background(), jid, name); err != nil {
		return SetGroupNameResponse{Success: false, Message: fmt.Sprintf("Error: %v", err)}
	}
	return SetGroupNameResponse{Success: true, Message: "Group name updated", JID: jid.String(), Name: name}
}

type SetGroupPhotoRequest struct {
	JID  string `json:"jid"`
	Path string `json:"path"` // absolute path to a JPEG/PNG image on the bridge host
}

type SetGroupPhotoResponse struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	JID       string `json:"jid,omitempty"`
	PictureID string `json:"picture_id,omitempty"`
}

func setGroupPhoto(client *whatsmeow.Client, jidStr, path string) SetGroupPhotoResponse {
	if !client.IsConnected() {
		return SetGroupPhotoResponse{Success: false, Message: "Not connected to WhatsApp"}
	}
	jid, err := parseGroupJID(jidStr)
	if err != nil {
		return SetGroupPhotoResponse{Success: false, Message: err.Error()}
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return SetGroupPhotoResponse{Success: false, Message: "Path is required"}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return SetGroupPhotoResponse{Success: false, Message: fmt.Sprintf("Cannot read file: %v", err)}
	}
	pictureID, err := client.SetGroupPhoto(context.Background(), jid, data)
	if err != nil {
		return SetGroupPhotoResponse{Success: false, Message: fmt.Sprintf("Error: %v", err)}
	}
	return SetGroupPhotoResponse{Success: true, Message: "Group photo updated", JID: jid.String(), PictureID: pictureID}
}

type UpdateGroupAdminsRequest struct {
	GroupJID     string   `json:"group_jid"`
	Participants []string `json:"participants"`
	Action       string   `json:"action"` // "promote" or "demote"
}

type UpdateGroupAdminsResponse struct {
	Success bool                `json:"success"`
	Message string              `json:"message"`
	Results []ParticipantResult `json:"results,omitempty"`
}

func updateGroupAdmins(client *whatsmeow.Client, req UpdateGroupAdminsRequest) UpdateGroupAdminsResponse {
	if !client.IsConnected() {
		return UpdateGroupAdminsResponse{Success: false, Message: "Not connected to WhatsApp"}
	}
	groupJID, err := parseGroupJID(req.GroupJID)
	if err != nil {
		return UpdateGroupAdminsResponse{Success: false, Message: err.Error()}
	}
	var change whatsmeow.ParticipantChange
	switch strings.ToLower(strings.TrimSpace(req.Action)) {
	case "promote":
		change = whatsmeow.ParticipantChangePromote
	case "demote":
		change = whatsmeow.ParticipantChangeDemote
	default:
		return UpdateGroupAdminsResponse{Success: false, Message: "Action must be 'promote' or 'demote'"}
	}
	if len(req.Participants) == 0 {
		return UpdateGroupAdminsResponse{Success: false, Message: "At least one participant is required"}
	}
	pjids := make([]types.JID, 0, len(req.Participants))
	for _, p := range req.Participants {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var pjid types.JID
		var perr error
		if strings.Contains(p, "@") {
			pjid, perr = types.ParseJID(p)
			if perr != nil {
				return UpdateGroupAdminsResponse{Success: false, Message: fmt.Sprintf("Invalid participant JID %q: %v", p, perr)}
			}
		} else {
			pjid = types.JID{User: strings.TrimPrefix(p, "+"), Server: "s.whatsapp.net"}
		}
		pjids = append(pjids, pjid)
	}
	if len(pjids) == 0 {
		return UpdateGroupAdminsResponse{Success: false, Message: "No valid participants after parsing"}
	}
	results, err := client.UpdateGroupParticipants(context.Background(), groupJID, pjids, change)
	if err != nil {
		return UpdateGroupAdminsResponse{Success: false, Message: fmt.Sprintf("Error updating admins: %v", err)}
	}
	out := make([]ParticipantResult, 0, len(results))
	for _, r := range results {
		out = append(out, ParticipantResult{JID: r.JID.String(), ErrorCode: r.Error})
	}
	return UpdateGroupAdminsResponse{Success: true, Message: "Admins updated", Results: out}
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ? WHERE id = ? AND chat_jid = ?",
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

// Get media info from the database
func (store *MessageStore) GetMediaInfo(id, chatJID string) (string, string, string, []byte, []byte, []byte, uint64, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64

	err := store.db.QueryRow(
		"SELECT media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length FROM messages WHERE id = ? AND chat_jid = ?",
		id, chatJID,
	).Scan(&mediaType, &filename, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength)

	return mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err
}

// MediaDownloader implements the whatsmeow.DownloadableMessage interface
type MediaDownloader struct {
	URL           string
	DirectPath    string
	MediaKey      []byte
	FileLength    uint64
	FileSHA256    []byte
	FileEncSHA256 []byte
	MediaType     whatsmeow.MediaType
}

// GetDirectPath implements the DownloadableMessage interface
func (d *MediaDownloader) GetDirectPath() string {
	return d.DirectPath
}

// GetURL implements the DownloadableMessage interface
func (d *MediaDownloader) GetURL() string {
	return d.URL
}

// GetMediaKey implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaKey() []byte {
	return d.MediaKey
}

// GetFileLength implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileLength() uint64 {
	return d.FileLength
}

// GetFileSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileSHA256() []byte {
	return d.FileSHA256
}

// GetFileEncSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileEncSHA256() []byte {
	return d.FileEncSHA256
}

// GetMediaType implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaType() whatsmeow.MediaType {
	return d.MediaType
}

// Function to download media from a message
func downloadMedia(client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string) (bool, string, string, string, error) {
	// Query the database for the message
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64
	var err error

	// First, check if we already have this file
	chatDir := fmt.Sprintf("store/%s", strings.ReplaceAll(chatJID, ":", "_"))
	localPath := ""

	// Get media info from the database
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err = messageStore.GetMediaInfo(messageID, chatJID)

	if err != nil {
		// Try to get basic info if extended info isn't available
		err = messageStore.db.QueryRow(
			"SELECT media_type, filename FROM messages WHERE id = ? AND chat_jid = ?",
			messageID, chatJID,
		).Scan(&mediaType, &filename)

		if err != nil {
			return false, "", "", "", fmt.Errorf("failed to find message: %v", err)
		}
	}

	// Check if this is a media message
	if mediaType == "" {
		return false, "", "", "", fmt.Errorf("not a media message")
	}

	// Create directory for the chat if it doesn't exist
	if err := os.MkdirAll(chatDir, 0755); err != nil {
		return false, "", "", "", fmt.Errorf("failed to create chat directory: %v", err)
	}

	// Generate a local path for the file
	localPath = fmt.Sprintf("%s/%s", chatDir, filename)

	// Get absolute path
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to get absolute path: %v", err)
	}

	// Check if file already exists
	if _, err := os.Stat(localPath); err == nil {
		// File exists, return it
		return true, mediaType, filename, absPath, nil
	}

	// If we don't have all the media info we need, we can't download
	if url == "" || len(mediaKey) == 0 || len(fileSHA256) == 0 || len(fileEncSHA256) == 0 || fileLength == 0 {
		return false, "", "", "", fmt.Errorf("incomplete media information for download")
	}

	fmt.Printf("Attempting to download media for message %s in chat %s...\n", messageID, chatJID)

	// Extract direct path from URL
	directPath := extractDirectPathFromURL(url)

	// Create a downloader that implements DownloadableMessage
	var waMediaType whatsmeow.MediaType
	switch mediaType {
	case "image":
		waMediaType = whatsmeow.MediaImage
	case "video":
		waMediaType = whatsmeow.MediaVideo
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "document":
		waMediaType = whatsmeow.MediaDocument
	default:
		return false, "", "", "", fmt.Errorf("unsupported media type: %s", mediaType)
	}

	downloader := &MediaDownloader{
		URL:           url,
		DirectPath:    directPath,
		MediaKey:      mediaKey,
		FileLength:    fileLength,
		FileSHA256:    fileSHA256,
		FileEncSHA256: fileEncSHA256,
		MediaType:     waMediaType,
	}

	// Download the media using whatsmeow client
	mediaData, err := client.Download(context.Background(), downloader)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to download media: %v", err)
	}

	// Save the downloaded media to file
	if err := os.WriteFile(localPath, mediaData, 0644); err != nil {
		return false, "", "", "", fmt.Errorf("failed to save media file: %v", err)
	}

	fmt.Printf("Successfully downloaded %s media to %s (%d bytes)\n", mediaType, absPath, len(mediaData))
	return true, mediaType, filename, absPath, nil
}

// Extract direct path from a WhatsApp media URL
func extractDirectPathFromURL(url string) string {
	// The direct path is typically in the URL, we need to extract it
	// Example URL: https://mmg.whatsapp.net/v/t62.7118-24/13812002_698058036224062_3424455886509161511_n.enc?ccb=11-4&oh=...

	// Find the path part after the domain
	parts := strings.SplitN(url, ".net/", 2)
	if len(parts) < 2 {
		return url // Return original URL if parsing fails
	}

	pathPart := parts[1]

	// Remove query parameters
	pathPart = strings.SplitN(pathPart, "?", 2)[0]

	// Create proper direct path format
	return "/" + pathPart
}

// Start a REST API server to expose the WhatsApp client functionality
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, port int) {
	// Handler for sending messages
	http.HandleFunc("/api/send", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req SendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}

		if req.Message == "" && req.MediaPath == "" {
			http.Error(w, "Message or media path is required", http.StatusBadRequest)
			return
		}

		fmt.Println("Received request to send message", req.Message, req.MediaPath)

		// Send the message
		success, message := sendWhatsAppMessage(client, req.Recipient, req.Message, req.MediaPath, req.Mentions)
		fmt.Println("Message sent", success, message)
		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Set appropriate status code
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}

		// Send response
		json.NewEncoder(w).Encode(SendMessageResponse{
			Success: success,
			Message: message,
		})
	})

	// Handler for downloading media
	http.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req DownloadMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.MessageID == "" || req.ChatJID == "" {
			http.Error(w, "Message ID and Chat JID are required", http.StatusBadRequest)
			return
		}

		// Download the media
		success, mediaType, filename, path, err := downloadMedia(client, messageStore, req.MessageID, req.ChatJID)

		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Handle download result
		if !success || err != nil {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(DownloadMediaResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to download media: %s", errMsg),
			})
			return
		}

		// Send successful response
		json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success:  true,
			Message:  fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename: filename,
			Path:     path,
		})
	})

	// Handler for leaving a group
	http.HandleFunc("/api/leave_group", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req LeaveGroupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		fmt.Printf("Received request to leave group %s\n", req.JID)
		resp := leaveWhatsAppGroup(client, req.JID)
		w.Header().Set("Content-Type", "application/json")
		if !resp.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Handler for adding participants to a group
	http.HandleFunc("/api/add_group_participants", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req AddGroupParticipantsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		fmt.Printf("Received request to add %d participants to %s\n", len(req.Participants), req.GroupJID)
		resp := addGroupParticipants(client, req)
		w.Header().Set("Content-Type", "application/json")
		if !resp.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Handler for fetching (or rotating) a group's invite link
	http.HandleFunc("/api/remove_group_participants", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req AddGroupParticipantsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		fmt.Printf("Received request to remove %d participants from %s\n", len(req.Participants), req.GroupJID)
		resp := removeGroupParticipants(client, req)
		w.Header().Set("Content-Type", "application/json")
		if !resp.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	http.HandleFunc("/api/get_group_invite_link", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req GetGroupInviteLinkRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		fmt.Printf("Received request for invite link for %s (reset=%t)\n", req.JID, req.Reset)
		resp := getGroupInviteLink(client, req.JID, req.Reset)
		w.Header().Set("Content-Type", "application/json")
		if !resp.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Handler for fetching full group info (including participant list)
	http.HandleFunc("/api/get_group_info", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req GetGroupInfoRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		fmt.Printf("Received request for group info on %s\n", req.JID)
		resp := getGroupInfo(client, req.JID)
		w.Header().Set("Content-Type", "application/json")
		if !resp.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Handler for creating a group
	http.HandleFunc("/api/create_group", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req CreateGroupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		fmt.Printf("Received request to create group %q with %d participants\n", req.Name, len(req.Participants))

		resp := createWhatsAppGroup(client, messageStore, req)

		w.Header().Set("Content-Type", "application/json")
		if !resp.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Handler for renaming a group
	http.HandleFunc("/api/set_group_name", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req SetGroupNameRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		fmt.Printf("Received request to set group name for %s to %q\n", req.JID, req.Name)
		resp := setGroupName(client, req.JID, req.Name)
		w.Header().Set("Content-Type", "application/json")
		if !resp.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Handler for setting group profile photo
	http.HandleFunc("/api/set_group_photo", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req SetGroupPhotoRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		fmt.Printf("Received request to set group photo for %s from %s\n", req.JID, req.Path)
		resp := setGroupPhoto(client, req.JID, req.Path)
		w.Header().Set("Content-Type", "application/json")
		if !resp.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Handler for listing all joined groups (live, from WhatsApp). Also upserts
	// each row into the local chats table so list_chats sees them too.
	http.HandleFunc("/api/list_joined_groups", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		fmt.Println("Received request to list joined groups")
		resp := listJoinedGroups(client, messageStore)
		w.Header().Set("Content-Type", "application/json")
		if !resp.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Handler for resolving a chat.whatsapp.com invite link to a group JID.
	// Auto-persists the resolved group into the local chats table on success.
	http.HandleFunc("/api/resolve_invite_link", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req ResolveInviteLinkRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		fmt.Printf("Received request to resolve invite link: %s\n", req.Link)
		resp := resolveInviteLink(client, messageStore, req.Link)
		w.Header().Set("Content-Type", "application/json")
		if !resp.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Handler for promoting/demoting group admins
	http.HandleFunc("/api/update_group_admins", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req UpdateGroupAdminsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		fmt.Printf("Received request to %s %d admins in group %s\n", req.Action, len(req.Participants), req.GroupJID)
		resp := updateGroupAdmins(client, req)
		w.Header().Set("Content-Type", "application/json")
		if !resp.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Start the server
	serverAddr := fmt.Sprintf(":%d", port)
	fmt.Printf("Starting REST API server on %s...\n", serverAddr)

	// Run server in a goroutine so it doesn't block
	go func() {
		if err := http.ListenAndServe(serverAddr, nil); err != nil {
			fmt.Printf("REST API server error: %v\n", err)
		}
	}()
}

func main() {
	// Set up logger
	logger := waLog.Stdout("Client", "INFO", true)
	logger.Infof("Starting WhatsApp client...")

	// Load privacy allow-list before any DB writes can happen
	loadAllowList()

	// Create database connection for storing session data
	dbLog := waLog.Stdout("Database", "INFO", true)

	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		logger.Errorf("Failed to create store directory: %v", err)
		return
	}

	container, err := sqlstore.New(context.Background(), "sqlite3", "file:store/whatsapp.db?_foreign_keys=on", dbLog)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		return
	}

	// Get device store - This contains session information
	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			// No device exists, create one
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			logger.Errorf("Failed to get device: %v", err)
			return
		}
	}

	// Create client instance
	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		logger.Errorf("Failed to create WhatsApp client")
		return
	}

	// Initialize message store
	messageStore, err := NewMessageStore()
	if err != nil {
		logger.Errorf("Failed to initialize message store: %v", err)
		return
	}
	defer messageStore.Close()

	// Setup event handling for messages and history sync
	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			// Process regular messages
			handleMessage(client, messageStore, v, logger)

		case *events.HistorySync:
			// Process history sync events
			handleHistorySync(client, messageStore, v, logger)

		case *events.Connected:
			logger.Infof("Connected to WhatsApp")
			// Bootstrap the local chats table with every group the account is
			// currently in. Without this, only groups that have sent a message
			// since pairing ever appear in list_chats — quiet groups stay
			// invisible forever. Runs in a goroutine to avoid blocking the
			// event handler.
			go func() {
				count, err := syncJoinedGroups(client, messageStore, logger)
				if err != nil {
					logger.Warnf("Initial joined-groups sync failed: %v", err)
					return
				}
				logger.Infof("Initial joined-groups sync complete: %d groups", count)
			}()

		case *events.LoggedOut:
			logger.Warnf("Device logged out, please scan QR code to log in again")
		}
	})

	// Create channel to track connection success
	connected := make(chan bool, 1)

	// Connect to WhatsApp
	if client.Store.ID == nil {
		// No ID stored, this is a new client, need to pair with phone
		pairPhone := strings.TrimPrefix(strings.TrimSpace(os.Getenv("WA_PAIR_PHONE")), "+")

		if pairPhone != "" {
			// Phone-pairing flow: connect first, then request 8-char pairing code
			err = client.Connect()
			if err != nil {
				logger.Errorf("Failed to connect: %v", err)
				return
			}
			// Wait briefly for socket to settle before PairPhone
			for i := 0; i < 20; i++ {
				if client.IsConnected() {
					break
				}
				time.Sleep(250 * time.Millisecond)
			}
			time.Sleep(1 * time.Second)
			code, err := client.PairPhone(context.Background(), pairPhone, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
			if err != nil {
				logger.Errorf("Failed to request pairing code: %v", err)
				return
			}
			fmt.Printf("\n\n>>> WhatsApp pairing code: %s <<<\n\n", code)
			if err := os.WriteFile("/tmp/wa-pair-code.txt", []byte(code), 0644); err != nil {
				logger.Warnf("Failed to write pair code: %v", err)
			}
			// Wait for pairing to complete (Store.ID becomes set after PairSuccess)
			deadline := time.Now().Add(3 * time.Minute)
			for time.Now().Before(deadline) {
				if client.Store.ID != nil && client.IsLoggedIn() {
					connected <- true
					break
				}
				time.Sleep(1 * time.Second)
			}
			select {
			case <-connected:
				fmt.Println("\nSuccessfully connected and authenticated!")
			default:
				logger.Errorf("Timeout waiting for pairing code entry")
				return
			}
		} else {
			// QR-pairing flow (default)
			qrChan, _ := client.GetQRChannel(context.Background())
			err = client.Connect()
			if err != nil {
				logger.Errorf("Failed to connect: %v", err)
				return
			}

			// Print QR code for pairing with phone
			for evt := range qrChan {
				if evt.Event == "code" {
					fmt.Println("\nScan this QR code with your WhatsApp app:")
					qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
					if err := os.WriteFile("/tmp/wa-qr-code.txt", []byte(evt.Code), 0644); err != nil {
						logger.Warnf("Failed to write QR code text: %v", err)
					}
				} else if evt.Event == "success" {
					connected <- true
					break
				}
			}

			// Wait for connection
			select {
			case <-connected:
				fmt.Println("\nSuccessfully connected and authenticated!")
			case <-time.After(3 * time.Minute):
				logger.Errorf("Timeout waiting for QR code scan")
				return
			}
		}
	} else {
		// Already logged in, just connect
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}
		connected <- true
	}

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		logger.Errorf("Failed to establish stable connection")
		return
	}

	fmt.Println("\n✓ Connected to WhatsApp! Type 'help' for commands.")

	// Start REST API server
	startRESTServer(client, messageStore, 8080)

	// Periodic joined-groups resync. The on-connect bootstrap catches drift
	// accumulated while the bridge was down; this catches drift that happens
	// while the bridge is up — renames, new joins, etc. — without needing
	// a restart. Interval is configurable via WHATSAPP_GROUP_RESYNC_INTERVAL.
	go runGroupResyncLoop(client, messageStore, logger)

	// Create a channel to keep the main goroutine alive
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("REST server is running. Press Ctrl+C to disconnect and exit.")

	// Wait for termination signal
	<-exitChan

	fmt.Println("Disconnecting...")
	// Disconnect client
	client.Disconnect()
}

// GetChatName determines the appropriate name for a chat based on JID and other info
func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	// First, check if chat already exists in database with a name
	var existingName string
	err := messageStore.db.QueryRow("SELECT name FROM chats WHERE jid = ?", chatJID).Scan(&existingName)
	if err == nil && existingName != "" {
		// Chat exists with a name, use that
		logger.Infof("Using existing chat name for %s: %s", chatJID, existingName)
		return existingName
	}

	// Need to determine chat name
	var name string

	if jid.Server == "g.us" {
		// This is a group chat
		logger.Infof("Getting name for group: %s", chatJID)

		// Use conversation data if provided (from history sync)
		if conversation != nil {
			// Extract name from conversation if available
			// This uses type assertions to handle different possible types
			var displayName, convName *string
			// Try to extract the fields we care about regardless of the exact type
			v := reflect.ValueOf(conversation)
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()

				// Try to find DisplayName field
				if displayNameField := v.FieldByName("DisplayName"); displayNameField.IsValid() && displayNameField.Kind() == reflect.Ptr && !displayNameField.IsNil() {
					dn := displayNameField.Elem().String()
					displayName = &dn
				}

				// Try to find Name field
				if nameField := v.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.Ptr && !nameField.IsNil() {
					n := nameField.Elem().String()
					convName = &n
				}
			}

			// Use the name we found
			if displayName != nil && *displayName != "" {
				name = *displayName
			} else if convName != nil && *convName != "" {
				name = *convName
			}
		}

		// If we didn't get a name, try group info
		if name == "" {
			groupInfo, err := client.GetGroupInfo(context.Background(), jid)
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				// Fallback name for groups
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}

		logger.Infof("Using group name: %s", name)
	} else {
		// This is an individual contact
		logger.Infof("Getting name for contact: %s", chatJID)

		// Just use contact info (full name)
		contact, err := client.Store.Contacts.GetContact(context.Background(), jid)
		if err == nil && contact.FullName != "" {
			name = contact.FullName
		} else if sender != "" {
			// Fallback to sender
			name = sender
		} else {
			// Last fallback to JID
			name = jid.User
		}

		logger.Infof("Using contact name: %s", name)
	}

	return name
}

// Handle history sync events
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) {
	fmt.Printf("Received history sync event with %d conversations\n", len(historySync.Data.Conversations))

	syncedCount := 0
	for _, conversation := range historySync.Data.Conversations {
		// Parse JID from the conversation
		if conversation.ID == nil {
			continue
		}

		chatJID := *conversation.ID

		// Try to parse the JID
		jid, err := types.ParseJID(chatJID)
		if err != nil {
			logger.Warnf("Failed to parse JID %s: %v", chatJID, err)
			continue
		}

		// Get appropriate chat name by passing the history sync conversation directly
		name := GetChatName(client, messageStore, jid, chatJID, conversation, "", logger)

		// Process messages
		messages := conversation.Messages
		if len(messages) > 0 {
			// Update chat with latest message timestamp
			latestMsg := messages[0]
			if latestMsg == nil || latestMsg.Message == nil {
				continue
			}

			// Get timestamp from message info
			timestamp := time.Time{}
			if ts := latestMsg.Message.GetMessageTimestamp(); ts != 0 {
				timestamp = time.Unix(int64(ts), 0)
			} else {
				continue
			}

			messageStore.StoreChat(chatJID, name, timestamp)

			// Store messages
			for _, msg := range messages {
				if msg == nil || msg.Message == nil {
					continue
				}

				// Extract text content
				var content string
				if msg.Message.Message != nil {
					if conv := msg.Message.Message.GetConversation(); conv != "" {
						content = conv
					} else if ext := msg.Message.Message.GetExtendedTextMessage(); ext != nil {
						content = ext.GetText()
					}
				}

				// Extract media info
				var mediaType, filename, url string
				var mediaKey, fileSHA256, fileEncSHA256 []byte
				var fileLength uint64

				if msg.Message.Message != nil {
					mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message)
				}

				// Log the message content for debugging
				logger.Infof("Message content: %v, Media Type: %v", content, mediaType)

				// Skip messages with no content and no media
				if content == "" && mediaType == "" {
					continue
				}

				// Determine sender
				var sender string
				isFromMe := false
				if msg.Message.Key != nil {
					if msg.Message.Key.FromMe != nil {
						isFromMe = *msg.Message.Key.FromMe
					}
					if !isFromMe && msg.Message.Key.Participant != nil && *msg.Message.Key.Participant != "" {
						sender = *msg.Message.Key.Participant
					} else if isFromMe {
						sender = client.Store.ID.User
					} else {
						sender = jid.User
					}
				} else {
					sender = jid.User
				}

				// Store message
				msgID := ""
				if msg.Message.Key != nil && msg.Message.Key.ID != nil {
					msgID = *msg.Message.Key.ID
				}

				// Get message timestamp
				timestamp := time.Time{}
				if ts := msg.Message.GetMessageTimestamp(); ts != 0 {
					timestamp = time.Unix(int64(ts), 0)
				} else {
					continue
				}

				err = messageStore.StoreMessage(
					msgID,
					chatJID,
					sender,
					content,
					timestamp,
					isFromMe,
					mediaType,
					filename,
					url,
					mediaKey,
					fileSHA256,
					fileEncSHA256,
					fileLength,
				)
				if err != nil {
					logger.Warnf("Failed to store history message: %v", err)
				} else {
					syncedCount++
					// Log successful message storage
					if mediaType != "" {
						logger.Infof("Stored message: [%s] %s -> %s: [%s: %s] %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, mediaType, filename, content)
					} else {
						logger.Infof("Stored message: [%s] %s -> %s: %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, content)
					}
				}
			}
		}
	}

	fmt.Printf("History sync complete. Stored %d messages.\n", syncedCount)
}

// Request history sync from the server
func requestHistorySync(client *whatsmeow.Client) {
	if client == nil {
		fmt.Println("Client is not initialized. Cannot request history sync.")
		return
	}

	if !client.IsConnected() {
		fmt.Println("Client is not connected. Please ensure you are connected to WhatsApp first.")
		return
	}

	if client.Store.ID == nil {
		fmt.Println("Client is not logged in. Please scan the QR code first.")
		return
	}

	// Build and send a history sync request
	historyMsg := client.BuildHistorySyncRequest(nil, 100)
	if historyMsg == nil {
		fmt.Println("Failed to build history sync request.")
		return
	}

	_, err := client.SendMessage(context.Background(), types.JID{
		Server: "s.whatsapp.net",
		User:   "status",
	}, historyMsg)

	if err != nil {
		fmt.Printf("Failed to request history sync: %v\n", err)
	} else {
		fmt.Println("History sync requested. Waiting for server response...")
	}
}

// analyzeOggOpus tries to extract duration and generate a simple waveform from an Ogg Opus file
func analyzeOggOpus(data []byte) (duration uint32, waveform []byte, err error) {
	// Try to detect if this is a valid Ogg file by checking for the "OggS" signature
	// at the beginning of the file
	if len(data) < 4 || string(data[0:4]) != "OggS" {
		return 0, nil, fmt.Errorf("not a valid Ogg file (missing OggS signature)")
	}

	// Parse Ogg pages to find the last page with a valid granule position
	var lastGranule uint64
	var sampleRate uint32 = 48000 // Default Opus sample rate
	var preSkip uint16 = 0
	var foundOpusHead bool

	// Scan through the file looking for Ogg pages
	for i := 0; i < len(data); {
		// Check if we have enough data to read Ogg page header
		if i+27 >= len(data) {
			break
		}

		// Verify Ogg page signature
		if string(data[i:i+4]) != "OggS" {
			// Skip until next potential page
			i++
			continue
		}

		// Extract header fields
		granulePos := binary.LittleEndian.Uint64(data[i+6 : i+14])
		pageSeqNum := binary.LittleEndian.Uint32(data[i+18 : i+22])
		numSegments := int(data[i+26])

		// Extract segment table
		if i+27+numSegments >= len(data) {
			break
		}
		segmentTable := data[i+27 : i+27+numSegments]

		// Calculate page size
		pageSize := 27 + numSegments
		for _, segLen := range segmentTable {
			pageSize += int(segLen)
		}

		// Check if we're looking at an OpusHead packet (should be in first few pages)
		if !foundOpusHead && pageSeqNum <= 1 {
			// Look for "OpusHead" marker in this page
			pageData := data[i : i+pageSize]
			headPos := bytes.Index(pageData, []byte("OpusHead"))
			if headPos >= 0 && headPos+12 < len(pageData) {
				// Found OpusHead, extract sample rate and pre-skip
				// OpusHead format: Magic(8) + Version(1) + Channels(1) + PreSkip(2) + SampleRate(4) + ...
				headPos += 8 // Skip "OpusHead" marker
				// PreSkip is 2 bytes at offset 10
				if headPos+12 <= len(pageData) {
					preSkip = binary.LittleEndian.Uint16(pageData[headPos+10 : headPos+12])
					sampleRate = binary.LittleEndian.Uint32(pageData[headPos+12 : headPos+16])
					foundOpusHead = true
					fmt.Printf("Found OpusHead: sampleRate=%d, preSkip=%d\n", sampleRate, preSkip)
				}
			}
		}

		// Keep track of last valid granule position
		if granulePos != 0 {
			lastGranule = granulePos
		}

		// Move to next page
		i += pageSize
	}

	if !foundOpusHead {
		fmt.Println("Warning: OpusHead not found, using default values")
	}

	// Calculate duration based on granule position
	if lastGranule > 0 {
		// Formula for duration: (lastGranule - preSkip) / sampleRate
		durationSeconds := float64(lastGranule-uint64(preSkip)) / float64(sampleRate)
		duration = uint32(math.Ceil(durationSeconds))
		fmt.Printf("Calculated Opus duration from granule: %f seconds (lastGranule=%d)\n",
			durationSeconds, lastGranule)
	} else {
		// Fallback to rough estimation if granule position not found
		fmt.Println("Warning: No valid granule position found, using estimation")
		durationEstimate := float64(len(data)) / 2000.0 // Very rough approximation
		duration = uint32(durationEstimate)
	}

	// Make sure we have a reasonable duration (at least 1 second, at most 300 seconds)
	if duration < 1 {
		duration = 1
	} else if duration > 300 {
		duration = 300
	}

	// Generate waveform
	waveform = placeholderWaveform(duration)

	fmt.Printf("Ogg Opus analysis: size=%d bytes, calculated duration=%d sec, waveform=%d bytes\n",
		len(data), duration, len(waveform))

	return duration, waveform, nil
}

// min returns the smaller of x or y
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// placeholderWaveform generates a synthetic waveform for WhatsApp voice messages
// that appears natural with some variability based on the duration
func placeholderWaveform(duration uint32) []byte {
	// WhatsApp expects a 64-byte waveform for voice messages
	const waveformLength = 64
	waveform := make([]byte, waveformLength)

	// Seed the random number generator for consistent results with the same duration
	rand.Seed(int64(duration))

	// Create a more natural looking waveform with some patterns and variability
	// rather than completely random values

	// Base amplitude and frequency - longer messages get faster frequency
	baseAmplitude := 35.0
	frequencyFactor := float64(min(int(duration), 120)) / 30.0

	for i := range waveform {
		// Position in the waveform (normalized 0-1)
		pos := float64(i) / float64(waveformLength)

		// Create a wave pattern with some randomness
		// Use multiple sine waves of different frequencies for more natural look
		val := baseAmplitude * math.Sin(pos*math.Pi*frequencyFactor*8)
		val += (baseAmplitude / 2) * math.Sin(pos*math.Pi*frequencyFactor*16)

		// Add some randomness to make it look more natural
		val += (rand.Float64() - 0.5) * 15

		// Add some fade-in and fade-out effects
		fadeInOut := math.Sin(pos * math.Pi)
		val = val * (0.7 + 0.3*fadeInOut)

		// Center around 50 (typical voice baseline)
		val = val + 50

		// Ensure values stay within WhatsApp's expected range (0-100)
		if val < 0 {
			val = 0
		} else if val > 100 {
			val = 100
		}

		waveform[i] = byte(val)
	}

	return waveform
}

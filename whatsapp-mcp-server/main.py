from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from whatsapp import (
    search_contacts as whatsapp_search_contacts,
    list_messages as whatsapp_list_messages,
    list_chats as whatsapp_list_chats,
    get_chat as whatsapp_get_chat,
    get_direct_chat_by_contact as whatsapp_get_direct_chat_by_contact,
    get_contact_chats as whatsapp_get_contact_chats,
    get_last_interaction as whatsapp_get_last_interaction,
    get_message_context as whatsapp_get_message_context,
    send_message as whatsapp_send_message,
    send_file as whatsapp_send_file,
    send_audio_message as whatsapp_audio_voice_message,
    download_media as whatsapp_download_media,
    create_group as whatsapp_create_group,
    leave_group as whatsapp_leave_group,
    add_group_participants as whatsapp_add_group_participants,
    remove_group_participants as whatsapp_remove_group_participants,
    get_group_info as whatsapp_get_group_info,
    get_group_invite_link as whatsapp_get_group_invite_link,
    set_group_name as whatsapp_set_group_name,
    set_group_photo as whatsapp_set_group_photo,
    update_group_admins as whatsapp_update_group_admins,
    list_joined_groups as whatsapp_list_joined_groups,
    resolve_invite_link as whatsapp_resolve_invite_link,
)

# Initialize FastMCP server
mcp = FastMCP("whatsapp")

@mcp.tool()
def search_contacts(query: str) -> List[Dict[str, Any]]:
    """Search WhatsApp contacts by name or phone number.
    
    Args:
        query: Search term to match against contact names or phone numbers
    """
    contacts = whatsapp_search_contacts(query)
    return contacts

@mcp.tool()
def list_messages(
    after: Optional[str] = None,
    before: Optional[str] = None,
    sender_phone_number: Optional[str] = None,
    chat_jid: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1
) -> List[Dict[str, Any]]:
    """Get WhatsApp messages matching specified criteria with optional context.
    
    Args:
        after: Optional ISO-8601 formatted string to only return messages after this date
        before: Optional ISO-8601 formatted string to only return messages before this date
        sender_phone_number: Optional phone number to filter messages by sender
        chat_jid: Optional chat JID to filter messages by chat
        query: Optional search term to filter messages by content
        limit: Maximum number of messages to return (default 20)
        page: Page number for pagination (default 0)
        include_context: Whether to include messages before and after matches (default True)
        context_before: Number of messages to include before each match (default 1)
        context_after: Number of messages to include after each match (default 1)
    """
    messages = whatsapp_list_messages(
        after=after,
        before=before,
        sender_phone_number=sender_phone_number,
        chat_jid=chat_jid,
        query=query,
        limit=limit,
        page=page,
        include_context=include_context,
        context_before=context_before,
        context_after=context_after
    )
    return messages

@mcp.tool()
def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active"
) -> List[Dict[str, Any]]:
    """Get WhatsApp chats matching specified criteria.
    
    Args:
        query: Optional search term to filter chats by name or JID
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
        include_last_message: Whether to include the last message in each chat (default True)
        sort_by: Field to sort results by, either "last_active" or "name" (default "last_active")
    """
    chats = whatsapp_list_chats(
        query=query,
        limit=limit,
        page=page,
        include_last_message=include_last_message,
        sort_by=sort_by
    )
    return chats

@mcp.tool()
def get_chat(chat_jid: str, include_last_message: bool = True) -> Dict[str, Any]:
    """Get WhatsApp chat metadata by JID.
    
    Args:
        chat_jid: The JID of the chat to retrieve
        include_last_message: Whether to include the last message (default True)
    """
    chat = whatsapp_get_chat(chat_jid, include_last_message)
    return chat

@mcp.tool()
def get_direct_chat_by_contact(sender_phone_number: str) -> Dict[str, Any]:
    """Get WhatsApp chat metadata by sender phone number.
    
    Args:
        sender_phone_number: The phone number to search for
    """
    chat = whatsapp_get_direct_chat_by_contact(sender_phone_number)
    return chat

@mcp.tool()
def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> List[Dict[str, Any]]:
    """Get all WhatsApp chats involving the contact.
    
    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    chats = whatsapp_get_contact_chats(jid, limit, page)
    return chats

@mcp.tool()
def get_last_interaction(jid: str) -> str:
    """Get most recent WhatsApp message involving the contact.
    
    Args:
        jid: The JID of the contact to search for
    """
    message = whatsapp_get_last_interaction(jid)
    return message

@mcp.tool()
def get_message_context(
    message_id: str,
    before: int = 5,
    after: int = 5
) -> Dict[str, Any]:
    """Get context around a specific WhatsApp message.
    
    Args:
        message_id: The ID of the message to get context for
        before: Number of messages to include before the target message (default 5)
        after: Number of messages to include after the target message (default 5)
    """
    context = whatsapp_get_message_context(message_id, before, after)
    return context

@mcp.tool()
def send_message(
    recipient: str,
    message: str,
    mentions: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Send a WhatsApp message to a person or group. For group chats use the JID.

    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        message: The message text to send
        mentions: Optional list of participants to @-tag in a group (phone numbers with
                 country code and no '+', or full JIDs). For the tag to render, include
                 the matching "@<number>" token in `message` (e.g. message "Hi @447581410358"
                 with mentions ["447581410358"]).

    Returns:
        A dictionary containing success status and a status message
    """
    # Validate input
    if not recipient:
        return {
            "success": False,
            "message": "Recipient must be provided"
        }

    # Call the whatsapp_send_message function with the unified recipient parameter
    success, status_message = whatsapp_send_message(recipient, message, mentions)
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def send_file(recipient: str, media_path: str) -> Dict[str, Any]:
    """Send a file such as a picture, raw audio, video or document via WhatsApp to the specified recipient. For group messages use the JID.
    
    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the media file to send (image, video, document)
    
    Returns:
        A dictionary containing success status and a status message
    """
    
    # Call the whatsapp_send_file function
    success, status_message = whatsapp_send_file(recipient, media_path)
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def send_audio_message(recipient: str, media_path: str) -> Dict[str, Any]:
    """Send any audio file as a WhatsApp audio message to the specified recipient. For group messages use the JID. If it errors due to ffmpeg not being installed, use send_file instead.
    
    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the audio file to send (will be converted to Opus .ogg if it's not a .ogg file)
    
    Returns:
        A dictionary containing success status and a status message
    """
    success, status_message = whatsapp_audio_voice_message(recipient, media_path)
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def download_media(message_id: str, chat_jid: str) -> Dict[str, Any]:
    """Download media from a WhatsApp message and get the local file path.
    
    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message
    
    Returns:
        A dictionary containing success status, a status message, and the file path if successful
    """
    file_path = whatsapp_download_media(message_id, chat_jid)
    
    if file_path:
        return {
            "success": True,
            "message": "Media downloaded successfully",
            "file_path": file_path
        }
    else:
        return {
            "success": False,
            "message": "Failed to download media"
        }

@mcp.tool()
def create_group(
    name: str,
    participants: List[str],
    announce: bool = False,
    locked: bool = False,
    ephemeral_seconds: int = 0,
    is_community: bool = False,
    community_parent_jid: str = "",
    join_approval_required: bool = False,
) -> Dict[str, Any]:
    """Create a new WhatsApp group.

    Args:
        name: Group subject (max 25 characters per WhatsApp's limit)
        participants: List of phone numbers (country code, no '+') or JIDs (e.g., "919866457501" or "919866457501@s.whatsapp.net").
                      Your own number is added automatically — do not include it.
        announce: If True, only admins can send messages
        locked: If True, only admins can edit group info (subject, description, picture)
        ephemeral_seconds: Disappearing-messages timer in seconds (0 disables; common values: 86400, 604800, 7776000)
        is_community: If True, create a community parent instead of a normal group
        community_parent_jid: If set, create this group as a sub-group inside the given community (mutually exclusive with is_community)
        join_approval_required: If True, new members need admin approval to join

    Returns:
        Dict with: success (bool), message (str), and on success: jid, name, participant_count, failed_adds (list of "<jid> (code N)" for participants the server refused).
    """
    success, message, details = whatsapp_create_group(
        name=name,
        participants=participants,
        announce=announce,
        locked=locked,
        ephemeral_seconds=ephemeral_seconds,
        is_community=is_community,
        community_parent_jid=community_parent_jid,
        join_approval_required=join_approval_required,
    )
    response: Dict[str, Any] = {"success": success, "message": message}
    if success and details:
        response.update(details)
    return response


@mcp.tool()
def add_group_participants(group_jid: str, participants: List[str]) -> Dict[str, Any]:
    """Add one or more participants to an existing WhatsApp group.

    You must be an admin of the group for this to succeed. Participants who can't be
    added directly (typically because of their "Who can add me to groups" privacy
    setting) are NOT silently dropped — the bridge automatically fetches the group's
    invite link and DMs it to that contact along with the group name. Each failure
    entry reports `invite_sent: true` (or `invite_error` if the DM itself failed).

    Args:
        group_jid: The group JID (must end with @g.us, e.g. "120363426272007458@g.us")
        participants: List of phone numbers (country code, no '+') or full JIDs to add

    Returns:
        Dict with: success (bool), message (str), added (list of {jid, error_code}),
        failed (list of {jid, error_code, invite_sent, invite_error}). `success` is
        True if at least one participant was added (or the call returned no failures).
    """
    success, message, details = whatsapp_add_group_participants(group_jid, participants)
    response: Dict[str, Any] = {"success": success, "message": message}
    if details:
        response.update(details)
    return response


@mcp.tool()
def remove_group_participants(group_jid: str, participants: List[str]) -> Dict[str, Any]:
    """Remove one or more participants from an existing WhatsApp group.

    You must be an admin of the group for this to succeed.

    Args:
        group_jid: The group JID (must end with @g.us, e.g. "120363426272007458@g.us")
        participants: List of phone numbers (country code, no '+') or full JIDs to remove

    Returns:
        Dict with: success (bool), message (str), removed (list of {jid, error_code}),
        failed (list of {jid, error_code}).
    """
    success, message, details = whatsapp_remove_group_participants(group_jid, participants)
    response: Dict[str, Any] = {"success": success, "message": message}
    if details:
        response.update(details)
    return response


@mcp.tool()
def get_group_info(jid: str) -> Dict[str, Any]:
    """Get full metadata for a WhatsApp group, including the participant list.

    Args:
        jid: The group JID (must end with @g.us, e.g. "120363426272007458@g.us")

    Returns:
        On success, a dict with: jid, name, topic, owner_jid, created (RFC3339),
        is_announce, is_locked, is_ephemeral, disappearing_timer, is_community,
        linked_parent_jid, join_approval_required, participant_count, and
        participants (list of {jid, phone_number, lid, is_admin, is_super_admin,
        display_name}). On failure, success=False with a message.
    """
    success, message, details = whatsapp_get_group_info(jid)
    response: Dict[str, Any] = {"success": success, "message": message}
    if success and details:
        response.update(details)
    return response


@mcp.tool()
def get_group_invite_link(jid: str, reset: bool = False) -> Dict[str, Any]:
    """Get a WhatsApp group's invite link (https://chat.whatsapp.com/<code>).

    You must be a member of the group (and typically an admin, depending on the
    group's settings) for this to succeed.

    Args:
        jid: The group JID (must end with @g.us, e.g. "120363426272007458@g.us")
        reset: If True, revoke the existing link and generate a new one. Old links
               will stop working immediately. Default False.

    Returns:
        Dict with success (bool), message (str), and on success: jid (str), link (str).
    """
    success, message, link = whatsapp_get_group_invite_link(jid, reset)
    response: Dict[str, Any] = {"success": success, "message": message}
    if success and link:
        response["jid"] = jid
        response["link"] = link
    return response


@mcp.tool()
def leave_group(jid: str) -> Dict[str, Any]:
    """Leave a WhatsApp group. Note: WhatsApp has no 'delete group' — leaving is the closest action.
    Other members will see "<you> left" and the group remains on their side.

    Args:
        jid: The group JID (must end with @g.us, e.g. "120363426272007458@g.us")

    Returns:
        Dict with success (bool) and message (str).
    """
    success, message = whatsapp_leave_group(jid)
    return {"success": success, "message": message}


@mcp.tool()
def set_group_name(jid: str, name: str) -> Dict[str, Any]:
    """Rename a WhatsApp group. You must be an admin.

    Args:
        jid: The group JID (must end with @g.us)
        name: New group name. Limit is 100 characters.

    Returns:
        Dict with success (bool), message (str), and on success: jid (str), name (str).
    """
    success, message, details = whatsapp_set_group_name(jid, name)
    response: Dict[str, Any] = {"success": success, "message": message}
    if success and details:
        response.update(details)
    return response


@mcp.tool()
def set_group_photo(jid: str, path: str) -> Dict[str, Any]:
    """Set a WhatsApp group's profile photo from a local image file. You must be admin.

    WhatsApp expects a JPEG, ideally square and ≤ ~640x640. PNGs (or oversized
    images) may be rejected with 'the given data is not a valid image' — convert
    to a square 640x640 JPEG first if needed.

    Args:
        jid: The group JID (must end with @g.us)
        path: Absolute path on the bridge host to the image file (JPEG preferred).

    Returns:
        Dict with success (bool), message (str), and on success: jid (str), picture_id (str).
    """
    success, message, details = whatsapp_set_group_photo(jid, path)
    response: Dict[str, Any] = {"success": success, "message": message}
    if success and details:
        response.update(details)
    return response


@mcp.tool()
def update_group_admins(group_jid: str, participants: List[str], action: str) -> Dict[str, Any]:
    """Promote or demote group admins. You must be a super-admin (group creator).

    Args:
        group_jid: The group JID (must end with @g.us)
        participants: List of phone numbers (country code, no '+') or full JIDs.
        action: Either 'promote' (make admin) or 'demote' (remove admin).

    Returns:
        Dict with success (bool), message (str), and results (list of {jid, error_code}).
        error_code is 0 on success; non-zero codes indicate per-participant failures.
    """
    success, message, details = whatsapp_update_group_admins(group_jid, participants, action)
    response: Dict[str, Any] = {"success": success, "message": message}
    if details:
        response.update(details)
    return response


@mcp.tool()
def list_joined_groups() -> Dict[str, Any]:
    """List every WhatsApp group the account is currently a member of, fetched
    live from WhatsApp (not from the local cache).

    The local SQLite cache that backs list_chats/search_contacts only learns
    about a group when a message event flows through it. Groups that have been
    quiet since the bridge was paired — or that were renamed while the bridge
    was offline — can be missing or stale there. This tool bypasses that cache
    by asking WhatsApp directly, and as a side effect upserts each returned
    row into the local chats table so subsequent list_chats / send_message
    calls find them too.

    Use this when:
    - A group you expect to see is not appearing in list_chats results.
    - You need a fresh ground-truth list of group memberships.
    - You suspect names are out of date after renames.

    Returns:
        Dict with success (bool), message (str), and on success: count (int),
        synced (int, how many were upserted into the local DB), and groups
        (list of {jid, name, topic, participant_count, is_announce, is_locked,
        is_community}).
    """
    success, message, details = whatsapp_list_joined_groups()
    response: Dict[str, Any] = {"success": success, "message": message}
    if success and details:
        response.update(details)
    return response


@mcp.tool()
def resolve_invite_link(link: str) -> Dict[str, Any]:
    """Resolve a https://chat.whatsapp.com/<code> invite link (or bare invite
    code) to the underlying group's JID and metadata, without joining the group.

    The chat.whatsapp.com invite page exposes only the invite code, never the
    group JID. WhatsApp keeps these separate so the JID can't be scraped from
    a shared link. This tool asks WhatsApp's servers to resolve the link and
    returns the JID + group info — and auto-upserts the result into the local
    chats table so the very next send_message / list_chats call sees it.

    Use this when a user shares an invite link and you need to send a message
    to that group (e.g. "send <message> to https://chat.whatsapp.com/<code>"):
    call this first to get the JID, then send_message with that JID.

    Args:
        link: A chat.whatsapp.com invite URL or the bare invite code at the
              end of one (e.g. either "https://chat.whatsapp.com/abc123XYZ"
              or just "abc123XYZ").

    Returns:
        Dict with success (bool), message (str), and on success: jid, name,
        topic, owner_jid, created (RFC3339), participant_count, plus group
        flags (is_announce, is_locked, is_ephemeral, is_community, etc.) and
        participants (when WhatsApp returns them — may be empty for non-members).
    """
    success, message, details = whatsapp_resolve_invite_link(link)
    response: Dict[str, Any] = {"success": success, "message": message}
    if success and details:
        response.update(details)
    return response


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
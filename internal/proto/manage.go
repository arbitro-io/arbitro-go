package proto

import (
	"encoding/binary"
	"encoding/json"
	"errors"
)

// packCold builds a cold-path (JSON body) frame.
func packCold(action uint16, seq uint64, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, HeaderSize+len(body))
	EncodeHeader(frame, Header{
		Action: action,
		Flags:  FlagAckReq,
		MsgLen: uint32(len(body)),
		Seq:    seq,
	})
	copy(frame[HeaderSize:], body)
	return frame, nil
}

// bytesArr converts a Go []byte to a JSON-compatible []int array.
// The Arbitro protocol encodes byte arrays as number arrays in JSON.
func bytesArr(b []byte) []int {
	if b == nil {
		return []int{}
	}
	arr := make([]int, len(b))
	for i, v := range b {
		arr[i] = int(v)
	}
	return arr
}

// --- Subscribe / Unsubscribe ---

type subscribePayload struct {
	ConsumerID     uint32  `json:"consumer_id"`
	SubscriptionID uint32  `json:"subscription_id"`
	Filters        [][]int `json:"filters"`
}

func EncodeSubscribe(seq uint64, consumerID uint32, filters [][]byte) ([]byte, error) {
	f := make([][]int, len(filters))
	for i, flt := range filters {
		f[i] = bytesArr(flt)
	}
	return packCold(ActionSubscribe, seq, subscribePayload{
		ConsumerID:     consumerID,
		SubscriptionID: 0,
		Filters:        f,
	})
}

type unsubscribePayload struct {
	ConsumerID uint32 `json:"consumer_id"`
}

func EncodeUnsubscribe(seq uint64, consumerID uint32) ([]byte, error) {
	return packCold(ActionUnsubscribe, seq, unsubscribePayload{ConsumerID: consumerID})
}

// --- Stream management ---

type createStreamPayload struct {
	Name                []int  `json:"name"`
	Filter              []int  `json:"filter"`
	MaxMsgs             uint64 `json:"max_msgs"`
	MaxBytes            uint64 `json:"max_bytes"`
	MaxAgeSecs          uint64 `json:"max_age_secs"`
	Replicas            uint32 `json:"replicas"`
	JournalKind         uint32 `json:"journal_kind"`
	Retention           uint32 `json:"retention"`
	Discard             uint32 `json:"discard"`
	IdempotencyWindowMs uint32 `json:"idempotency_window_ms"`
}

func EncodeCreateStream(seq uint64, name, filter []byte, maxMsgs, maxBytes, maxAgeSecs uint64, replicas, journalKind, retention, discard, idempotencyWindowMs uint32) ([]byte, error) {
	return packCold(ActionCreateStream, seq, createStreamPayload{
		Name:                bytesArr(name),
		Filter:              bytesArr(filter),
		MaxMsgs:             maxMsgs,
		MaxBytes:            maxBytes,
		MaxAgeSecs:          maxAgeSecs,
		Replicas:            replicas,
		JournalKind:         journalKind,
		Retention:           retention,
		Discard:             discard,
		IdempotencyWindowMs: idempotencyWindowMs,
	})
}

type deleteStreamPayload struct {
	Name       []int `json:"name"`
	DeleteData bool  `json:"delete_data"`
}

func EncodeDeleteStream(seq uint64, name []byte, deleteData bool) ([]byte, error) {
	return packCold(ActionDeleteStream, seq, deleteStreamPayload{
		Name:       bytesArr(name),
		DeleteData: deleteData,
	})
}

type nameOnlyPayload struct {
	Name []int `json:"name"`
}

func EncodeGetStream(seq uint64, name []byte) ([]byte, error) {
	return packCold(ActionGetStream, seq, nameOnlyPayload{Name: bytesArr(name)})
}

type listStreamsPayload struct {
	Offset uint32 `json:"offset"`
	Limit  uint32 `json:"limit"`
}

// EncodeListStreams sends a paginated ListStreams request.
// Mirrors arbitro-proto v2::cold::ListStreams { offset, limit }.
func EncodeListStreams(seq uint64, offset, limit uint32) ([]byte, error) {
	return packCold(ActionListStreams, seq, listStreamsPayload{
		Offset: offset,
		Limit:  limit,
	})
}

func EncodePurgeStream(seq uint64, name []byte) ([]byte, error) {
	return packCold(ActionPurgeStream, seq, nameOnlyPayload{Name: bytesArr(name)})
}

type drainSubjectPayload struct {
	Name    []int `json:"name"`
	Subject []int `json:"subject"`
}

func EncodeDrainSubject(seq uint64, name, subject []byte) ([]byte, error) {
	return packCold(ActionDrainSubject, seq, drainSubjectPayload{
		Name:    bytesArr(name),
		Subject: bytesArr(subject),
	})
}

type deleteMessagePayload struct {
	Name []int  `json:"name"`
	Seq  uint64 `json:"seq"`
}

func EncodeDeleteMessage(seq uint64, name []byte, msgSeq uint64) ([]byte, error) {
	return packCold(ActionDeleteMessage, seq, deleteMessagePayload{
		Name: bytesArr(name),
		Seq:  msgSeq,
	})
}

// --- Consumer management ---

type SubjectLimitJSON struct {
	Pattern []int  `json:"pattern"`
	Limit   uint32 `json:"limit"`
}

type createConsumerPayload struct {
	StreamID      uint32             `json:"stream_id"`
	Name          []int              `json:"name"`
	Group         []int              `json:"group"`
	Subject       []int              `json:"subject"`
	MaxInflight   uint16             `json:"max_inflight"`
	AckPolicy     uint32             `json:"ack_policy"`
	DeliverPolicy uint32             `json:"deliver_policy"`
	DeliverMode   uint32             `json:"deliver_mode"`
	AckWaitMs     uint32             `json:"ack_wait_ms"`
	StartSeq      uint64             `json:"start_seq"`
	SubjectLimits []SubjectLimitJSON `json:"subject_limits"`
}

func EncodeCreateConsumer(seq uint64, streamID uint32, name, group, subject []byte, maxInflight uint16, ackPolicy, deliverPolicy, deliverMode, ackWaitMs uint32, startSeq uint64, subjectLimits []SubjectLimitJSON) ([]byte, error) {
	// A nil subjectLimits marshals to JSON `null`, but the broker's
	// cold::CreateConsumer.subject_limits field is a plain (non-Option)
	// Vec<SubjectLimit> — serde rejects `null` there and the broker replies
	// with a generic InternalError (0x0502), not a helpful parse error. An
	// empty-but-non-nil slice marshals to `[]`, which deserializes cleanly
	// as an empty Vec. Every caller that doesn't set per-subject limits
	// (nil is the natural zero value) benefits from this normalization.
	if subjectLimits == nil {
		subjectLimits = []SubjectLimitJSON{}
	}
	return packCold(ActionCreateConsumer, seq, createConsumerPayload{
		StreamID:      streamID,
		Name:          bytesArr(name),
		Group:         bytesArr(group),
		Subject:       bytesArr(subject),
		MaxInflight:   maxInflight,
		AckPolicy:     ackPolicy,
		DeliverPolicy: deliverPolicy,
		DeliverMode:   deliverMode,
		AckWaitMs:     ackWaitMs,
		StartSeq:      startSeq,
		SubjectLimits: subjectLimits,
	})
}

type deleteConsumerPayload struct {
	ConsumerID uint32 `json:"consumer_id"`
}

// EncodeDeleteConsumer builds a DeleteConsumer cold frame. The wire body is a
// single `consumer_id` (see arbitro-proto v2::cold DeleteConsumer). The caller
// must resolve the consumer's numeric ID from (stream, name) before calling.
func EncodeDeleteConsumer(seq uint64, consumerID uint32) ([]byte, error) {
	return packCold(ActionDeleteConsumer, seq, deleteConsumerPayload{
		ConsumerID: consumerID,
	})
}

type getConsumerPayload struct {
	StreamID uint32 `json:"stream_id"`
	Name     []int  `json:"name"`
}

func EncodeGetConsumer(seq uint64, streamID uint32, name []byte) ([]byte, error) {
	return packCold(ActionGetConsumer, seq, getConsumerPayload{
		StreamID: streamID,
		Name:     bytesArr(name),
	})
}

type listConsumersPayload struct {
	StreamID uint32 `json:"stream_id"`
	Offset   uint32 `json:"offset"`
	Limit    uint32 `json:"limit"`
}

// EncodeListConsumers sends a paginated ListConsumers request. Passing
// streamID=0 lists consumers across every stream (broker semantic).
// Mirrors arbitro-proto v2::cold::ListConsumers { stream_id, offset, limit }.
func EncodeListConsumers(seq uint64, streamID, offset, limit uint32) ([]byte, error) {
	return packCold(ActionListConsumers, seq, listConsumersPayload{
		StreamID: streamID,
		Offset:   offset,
		Limit:    limit,
	})
}

type consumerStatsPayload struct {
	ConsumerID uint32 `json:"consumer_id"`
}

// EncodeConsumerStats sends a ConsumerStats request (action 0x0505). The
// broker's reply is a RepOk whose 8-byte body packs the live pending-ack
// count in place of the usual ref_seq — see arbitro-client-tokio's
// client.rs get_pending / manage/mod.rs consumer_stats. Read the count back
// with RepOkRefSeq, same as any other RepOk body.
func EncodeConsumerStats(seq uint64, consumerID uint32) ([]byte, error) {
	return packCold(ActionConsumerStats, seq, consumerStatsPayload{ConsumerID: consumerID})
}

// EncodePauseConsumer / EncodeResumeConsumer address the consumer by ID, not
// by (stream, name). arbitro-proto v2::cold declares both bodies as
// `{ consumer_id: u32 }` (cold/mod.rs:130), and the broker answers a body it
// cannot deserialize with InternalError — so sending {stream_id, name} here
// fails every pause/resume rather than producing a wrong-but-plausible result.
// Callers resolve the ID with GetConsumer first.
func EncodePauseConsumer(seq uint64, consumerID uint32) ([]byte, error) {
	return packCold(ActionPauseConsumer, seq, consumerStatsPayload{ConsumerID: consumerID})
}

func EncodeResumeConsumer(seq uint64, consumerID uint32) ([]byte, error) {
	return packCold(ActionResumeConsumer, seq, consumerStatsPayload{ConsumerID: consumerID})
}

// --- Cron ---

// createCronPayload mirrors arbitro-proto wire::cron::CreateCronBody:
// {"name": String, "every": String, "tz": Option<String>, "timeout_ms": u32, "overlap": bool}.
// name/every are JSON strings (standard json.Marshal UTF-8 encoding) — NOT
// byte arrays, which would serialize to base64 and break wire compatibility.
type createCronPayload struct {
	Name      string  `json:"name"`
	Every     string  `json:"every"`
	Tz        *string `json:"tz,omitempty"`
	TimeoutMs uint32  `json:"timeout_ms"`
	Overlap   bool    `json:"overlap"`
}

// EncodeCreateCron builds a CreateCron cold frame. every is the cron
// expression, timeoutMs is the handler timeout (0 = no timeout, per
// arbitro-proto CreateCronBody.timeout_ms semantics).
func EncodeCreateCron(seq uint64, name []byte, every, tz string, timeoutMs uint32, overlap bool) ([]byte, error) {
	var tzPtr *string
	if tz != "" {
		tzPtr = &tz
	}
	return packCold(ActionCreateCron, seq, createCronPayload{
		Name:      string(name),
		Every:     every,
		Tz:        tzPtr,
		TimeoutMs: timeoutMs,
		Overlap:   overlap,
	})
}

// EncodeDeleteCron carries the cron name as RAW body bytes, not JSON.
// DeleteCron is the one cron frame that skips serde: arbitro-proto's
// encode_delete_cron (wire/cron.rs:49) appends the name straight after the
// header, and the broker reads it back as `&frame[HEADER_SIZE..]`. A JSON
// body would be looked up verbatim as a cron name, miss, and come back as
// InternalError.
func EncodeDeleteCron(seq uint64, name []byte) ([]byte, error) {
	frame := make([]byte, HeaderSize+len(name))
	EncodeHeader(frame, Header{
		Action: ActionDeleteCron,
		Flags:  FlagAckReq,
		MsgLen: uint32(len(name)),
		Seq:    seq,
	})
	copy(frame[HeaderSize:], name)
	return frame, nil
}

func EncodeListCrons(seq uint64) ([]byte, error) {
	return packCold(ActionListCrons, seq, struct{}{})
}

// --- List replies (binary body — not JSON, per dispatch_v2.rs) ---

// StreamListEntry is one entry in a ListStreams reply.
type StreamListEntry struct {
	StreamID uint32
	Name     []byte
}

// ConsumerListEntry is one entry in a ListConsumers reply.
type ConsumerListEntry struct {
	ConsumerID uint32
	StreamID   uint32
	QueueID    uint32
	Paused     bool
}

// DecodeListStreamsBody parses the ListStreams reply body:
//
//	count u32 LE, then N × (wire_id u32 LE, name_len u16 LE, name bytes)
//
// This mirrors dispatch_v2::v2_list_streams on the server side.
func DecodeListStreamsBody(body []byte) ([]StreamListEntry, error) {
	if len(body) < 4 {
		return nil, errors.New("list streams: body too short")
	}
	count := binary.LittleEndian.Uint32(body[0:4])
	off := 4
	out := make([]StreamListEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+6 > len(body) {
			return nil, errors.New("list streams: truncated entry header")
		}
		id := binary.LittleEndian.Uint32(body[off : off+4])
		nameLen := int(binary.LittleEndian.Uint16(body[off+4 : off+6]))
		off += 6
		if off+nameLen > len(body) {
			return nil, errors.New("list streams: truncated name")
		}
		name := make([]byte, nameLen)
		copy(name, body[off:off+nameLen])
		off += nameLen
		out = append(out, StreamListEntry{StreamID: id, Name: name})
	}
	return out, nil
}

// DecodeListConsumersBody parses the ListConsumers reply body:
//
//	count u32 LE, then N × (consumer_id u32, stream_id u32, queue_id u32, paused u8) = 13B
//
// This mirrors dispatch_v2::v2_list_consumers on the server side.
func DecodeListConsumersBody(body []byte) ([]ConsumerListEntry, error) {
	if len(body) < 4 {
		return nil, errors.New("list consumers: body too short")
	}
	count := binary.LittleEndian.Uint32(body[0:4])
	const entrySize = 13
	if len(body) < 4+int(count)*entrySize {
		return nil, errors.New("list consumers: body truncated")
	}
	out := make([]ConsumerListEntry, 0, count)
	off := 4
	for i := uint32(0); i < count; i++ {
		cid := binary.LittleEndian.Uint32(body[off : off+4])
		sid := binary.LittleEndian.Uint32(body[off+4 : off+8])
		qid := binary.LittleEndian.Uint32(body[off+8 : off+12])
		paused := body[off+12] != 0
		off += entrySize
		out = append(out, ConsumerListEntry{
			ConsumerID: cid,
			StreamID:   sid,
			QueueID:    qid,
			Paused:     paused,
		})
	}
	return out, nil
}

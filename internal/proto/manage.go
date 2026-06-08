package proto

import "encoding/json"

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
	Name                 []int  `json:"name"`
	Filter               []int  `json:"filter"`
	MaxMsgs              uint64 `json:"max_msgs"`
	MaxBytes             uint64 `json:"max_bytes"`
	MaxAgeSecs           uint64 `json:"max_age_secs"`
	Replicas             uint32 `json:"replicas"`
	JournalKind          uint32 `json:"journal_kind"`
	Retention            uint32 `json:"retention"`
	Discard              uint32 `json:"discard"`
	IdempotencyWindowMs  uint32 `json:"idempotency_window_ms"`
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

func EncodeListStreams(seq uint64) ([]byte, error) {
	return packCold(ActionListStreams, seq, struct{}{})
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
	StreamID uint32 `json:"stream_id"`
	Name     []int  `json:"name"`
}

func EncodeDeleteConsumer(seq uint64, streamID uint32, name []byte) ([]byte, error) {
	return packCold(ActionDeleteConsumer, seq, deleteConsumerPayload{
		StreamID: streamID,
		Name:     bytesArr(name),
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

func EncodeListConsumers(seq uint64) ([]byte, error) {
	return packCold(ActionListConsumers, seq, struct{}{})
}

func EncodePauseConsumer(seq uint64, streamID uint32, name []byte) ([]byte, error) {
	return packCold(ActionPauseConsumer, seq, getConsumerPayload{
		StreamID: streamID,
		Name:     bytesArr(name),
	})
}

func EncodeResumeConsumer(seq uint64, streamID uint32, name []byte) ([]byte, error) {
	return packCold(ActionResumeConsumer, seq, getConsumerPayload{
		StreamID: streamID,
		Name:     bytesArr(name),
	})
}

// --- Cron ---

type createCronPayload struct {
	Name     []int  `json:"name"`
	CronExpr string `json:"cron_expr"`
	Tz       string `json:"tz"`
	Overlap  bool   `json:"overlap"`
}

func EncodeCreateCron(seq uint64, name []byte, cronExpr, tz string, overlap bool) ([]byte, error) {
	return packCold(ActionCreateCron, seq, createCronPayload{
		Name:     bytesArr(name),
		CronExpr: cronExpr,
		Tz:       tz,
		Overlap:  overlap,
	})
}

func EncodeDeleteCron(seq uint64, name []byte) ([]byte, error) {
	return packCold(ActionDeleteCron, seq, nameOnlyPayload{Name: bytesArr(name)})
}

func EncodeListCrons(seq uint64) ([]byte, error) {
	return packCold(ActionListCrons, seq, struct{}{})
}

package proto

// Action codes — V2 binary wire protocol.
// All values are uint16 LE in the frame header at offset 0.
const (
	// Handshake (not framed — raw 8 bytes)
	ActionHello uint16 = 0x0001
	ActionAuth  uint16 = 0x0002

	// Publish (hot path, client → server)
	ActionPublish             uint16 = 0x0101
	ActionPublishAccumulate   uint16 = 0x0102
	ActionPublishBatch        uint16 = 0x0103
	ActionPublishWithReply    uint16 = 0x0104
	ActionPublishWithHeaders  uint16 = 0x0105
	ActionPublishBatchHeaders uint16 = 0x0106

	// Delivery & acknowledgment (hot path)
	ActionDeliver      uint16 = 0x0200
	ActionAck          uint16 = 0x0201
	ActionNack         uint16 = 0x0202
	ActionRepOk        uint16 = 0x0203
	ActionRepError     uint16 = 0x0204
	ActionRepBatch     uint16 = 0x0205
	ActionBatchAck     uint16 = 0x0206
	ActionFanoutBatch  uint16 = 0x0207
	ActionAckSync      uint16 = 0x0208
	ActionBatchAckSync uint16 = 0x0209
	ActionBatchNack    uint16 = 0x020A
	ActionAckTerm      uint16 = 0x020B

	// Subscribe (cold path, JSON body)
	ActionSubscribe   uint16 = 0x0301
	ActionUnsubscribe uint16 = 0x0302

	// Stream management (cold path, JSON body)
	ActionCreateStream  uint16 = 0x0401
	ActionDeleteStream  uint16 = 0x0402
	ActionGetStream     uint16 = 0x0403
	ActionListStreams   uint16 = 0x0404
	ActionPurgeStream   uint16 = 0x0405
	ActionDrainSubject  uint16 = 0x0406
	ActionDeleteMessage uint16 = 0x0407

	// Consumer management (cold path, JSON body)
	ActionCreateConsumer  uint16 = 0x0501
	ActionDeleteConsumer  uint16 = 0x0502
	ActionGetConsumer     uint16 = 0x0503
	ActionListConsumers   uint16 = 0x0504
	ActionConsumerStats   uint16 = 0x0505
	ActionPauseConsumer   uint16 = 0x0506
	ActionResumeConsumer  uint16 = 0x0507

	// System
	ActionPing       uint16 = 0x0601
	ActionPong       uint16 = 0x0602
	ActionConnect    uint16 = 0x0603
	ActionConnected  uint16 = 0x0604
	ActionDisconnect uint16 = 0x0605

	// Cron (cold path)
	ActionCreateCron uint16 = 0x0701
	ActionDeleteCron uint16 = 0x0702
	ActionListCrons  uint16 = 0x0703
	ActionCronFire   uint16 = 0x0704
	ActionCronAck    uint16 = 0x0705

	// Delayed publish (hot path)
	ActionPublishDelayed uint16 = 0x0801
)

// Frame header flags (offset +2)
const (
	FlagNone         byte = 0x00
	FlagAckReq       byte = 0x01
	FlagDup          byte = 0x02
	FlagPriorityHigh byte = 0x04
)

// Entry flags (offset +3)
const (
	EntryFlagNone           byte = 0x00
	EntryFlagRetain         byte = 0x01
	EntryFlagCompressed     byte = 0x02
	EntryFlagNoBackpressure byte = 0x04
)

// Handshake capabilities bitmask (uint16 LE at hello offset +6)
const (
	CapHeaders           uint16 = 0x0001
	CapReply             uint16 = 0x0002
	CapBatchHeaders      uint16 = 0x0004
	CapCompressedPayload uint16 = 0x0008
)

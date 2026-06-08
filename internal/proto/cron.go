package proto

// EncodeCronAck builds a CronAck frame (JSON body).
// Sent client → broker after processing a CronFire.
func EncodeCronAck(seq uint64, name []byte, ok bool) ([]byte, error) {
	type cronAckPayload struct {
		Name []int `json:"name"`
		OK   bool  `json:"ok"`
	}
	return packCold(ActionCronAck, seq, cronAckPayload{
		Name: bytesArr(name),
		OK:   ok,
	})
}

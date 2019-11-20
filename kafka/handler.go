package kafka

// services consuming from Kafka should meet this contract
type Handler interface {
	Message(*ConsumerMessage) error
}

var NoOpHandler = &noOpHandler{}

// no-op message handler impl
type noOpHandler struct{}

func (h *noOpHandler) Message(kmsg *ConsumerMessage) error {
	return nil
}

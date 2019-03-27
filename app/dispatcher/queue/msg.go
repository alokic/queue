package queue

type (
	//Msg is a holder for queue message
	Msg struct {
		Data []byte
		meta interface{} //handle for queue message.e.g for kafka its topicpartition.
	}
)

// NewMsg is constructor of msg.
func NewMsg(d []byte, m interface{}) Msg {
	return Msg{Data: d, meta: m}
}

// Meta returns meta of Msg.
func (m *Msg) Meta() interface{} {
	return m.meta
}

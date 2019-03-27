package kafka

import "encoding/json"

//MockProducer struct.
type MockProducer struct {
	WriteStream chan struct{}
}

//Write to stream.
func (mp *MockProducer) Write(d []json.RawMessage) error {
	mp.WriteStream <- struct{}{}
	return nil
}

//Close stream.
func (mp *MockProducer) Close() error {
	if mp.WriteStream != nil {
		close(mp.WriteStream)
	}
	return nil
}

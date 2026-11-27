package zerobus

import "log"

const (
	// messageIDStreamBits is the number of high bits that hold the lane index.
	messageIDStreamBits = 6
	// messageIDOffsetBits is the number of low bits that hold the lane offset.
	messageIDOffsetBits = 64 - messageIDStreamBits
	// messageIDOffsetMask selects the lane offset bits of a MessageID.
	messageIDOffsetMask = int64(1)<<messageIDOffsetBits - 1
)

// MaxMultiplexedStreams is the maximum number of lanes in one multiplexed
// stream.
//
// Multiplexed streams are a Beta API.
const MaxMultiplexedStreams = 1 << messageIDStreamBits

// MessageID identifies one ingest call on a multiplexed stream. Match it with
// the IDs that a MultiplexedAckCallback receives. int64(id) is the raw value.
//
// Unlike a Stream offset, MessageID values are not ordered. Use an ID only
// with the multiplexed stream that returned it.
//
// Multiplexed streams are a Beta API.
type MessageID int64

// newMessageID packs a lane index and a lane offset. The caller keeps
// streamIndex below MaxMultiplexedStreams and offset non-negative; offset bits
// above the low 58 are dropped.
func newMessageID(streamIndex int, offset int64) MessageID {
	return MessageID(int64(uint64(streamIndex)<<messageIDOffsetBits) | offset&messageIDOffsetMask)
}

// streamIndex returns the index of the lane that took the ingest call.
func (id MessageID) streamIndex() int {
	return int(uint64(id) >> messageIDOffsetBits)
}

// offset returns the offset of the ingest call within its lane.
func (id MessageID) offset() int64 {
	return int64(id) & messageIDOffsetMask
}

// Auth selects how the lanes of a multiplexed stream authenticate. Create one
// with OAuth or Headers.
//
// Multiplexed streams are a Beta API.
type Auth interface {
	isAuth()
}

type oauthAuth struct {
	clientID, clientSecret string
}

type headersAuth struct {
	provider HeadersProvider
}

func (oauthAuth) isAuth()   {}
func (headersAuth) isAuth() {}

// OAuth authenticates with Unity Catalog OAuth client credentials. Each lane
// gets its own OAuth provider; the lanes share the SDK's token cache.
//
// Multiplexed streams are a Beta API.
func OAuth(clientID, clientSecret string) Auth {
	return oauthAuth{clientID: clientID, clientSecret: clientSecret}
}

// Headers authenticates every lane with one custom HeadersProvider. The lanes
// call it concurrently, so it must be safe for concurrent use.
//
// Multiplexed streams are a Beta API.
func Headers(provider HeadersProvider) Auth {
	return headersAuth{provider: provider}
}

// MultiplexedAckCallback receives asynchronous acknowledgement and error
// notifications for the ingest calls of a multiplexed stream. Register one
// with WithMultiplexedAckCallback.
//
// Each lane calls the callback from its own worker, so calls for different
// lanes can run concurrently and have no global order. Calls for one lane
// arrive in offset order. Implementations must be safe for concurrent use and
// should return promptly. A panic in a callback is recovered and logged with
// the standard log package, and later callbacks still run.
//
// Multiplexed stream methods, including Close, may be called from a callback.
// Callbacks can still run after Close returns.
//
// Multiplexed streams are a Beta API.
type MultiplexedAckCallback interface {
	// OnAck reports that the ingest call with this ID is durable.
	OnAck(id MessageID)
	// OnError reports that the ingest call with this ID failed.
	OnError(id MessageID, err error)
}

// WithMultiplexedAckCallback registers a callback for asynchronous ack and
// error notifications on a multiplexed stream. It is an alternative to
// blocking on Flush; both may be used together.
//
// It is only valid with CreateMultiplexedStream. CreateStream and
// CreateStreamWithProvider reject it with a non-retryable error.
//
// Multiplexed streams are a Beta API.
func WithMultiplexedAckCallback(cb MultiplexedAckCallback) StreamOption {
	return func(c *streamConfig) { c.muxCallback = cb }
}

// multiplexedAckAdapter is the AckCallback of one lane. It maps each lane
// offset to the MessageID that the multiplexed stream returned for it.
type multiplexedAckAdapter struct {
	streamIndex int
	callback    MultiplexedAckCallback
}

func (a multiplexedAckAdapter) OnAck(offset int64) {
	id := newMessageID(a.streamIndex, offset)
	defer recoverMultiplexedCallback("OnAck", id)
	a.callback.OnAck(id)
}

func (a multiplexedAckAdapter) OnError(offset int64, err error) {
	id := newMessageID(a.streamIndex, offset)
	defer recoverMultiplexedCallback("OnError", id)
	a.callback.OnError(id, err)
}

// recoverMultiplexedCallback logs a panic recovered from a
// MultiplexedAckCallback method, so later callbacks still run.
func recoverMultiplexedCallback(method string, id MessageID) {
	if r := recover(); r != nil {
		log.Printf("zerobus: MultiplexedAckCallback.%s(message ID %d) panicked: %v", method, int64(id), r)
	}
}

// multiplexedAckCallback returns the AckCallback for the lane at streamIndex,
// or nil when cb is nil.
func multiplexedAckCallback(streamIndex int, cb MultiplexedAckCallback) AckCallback {
	if cb == nil {
		return nil
	}
	return multiplexedAckAdapter{streamIndex: streamIndex, callback: cb}
}

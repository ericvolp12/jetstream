package consumer

type Event struct {
	Did       string `json:"did"`
	TimeUS    int64  `json:"time_us"`
	EventType string `json:"type"`
	Payload   any    `json:"payload"` // Commit, Account, Identity
}

type Commit struct {
	Rev        string `json:"rev,omitempty"`
	OpType     string `json:"type"`
	Collection string `json:"collection,omitempty"`
	RKey       string `json:"rkey,omitempty"`
	Record     any    `json:"record,omitempty"`
}

var (
	EventCommit   = "com"
	EventAccount  = "acc"
	EventIdentity = "id"

	CommitCreateRecord = "c"
	CommitUpdateRecord = "u"
	CommitDeleteRecord = "d"
)

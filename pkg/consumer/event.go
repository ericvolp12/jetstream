package consumer

type Event struct {
	Did        string `json:"did"`
	Seq        int64  `json:"seq"`
	OpType     string `json:"opType"`
	Collection string `json:"collection,omitempty"`
	RKey       string `json:"rkey,omitempty"`
	Cid        string `json:"cid,omitempty"`

	Record any `json:"record,omitempty"`
}

var (
	EvtCreateRecord = "c"
	EvtUpdateRecord = "u"
	EvtDeleteRecord = "d"
)

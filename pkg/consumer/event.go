package consumer

type Event struct {
	Did    string      `json:"did"`
	Seq    int64       `json:"seq"`
	Type   string      `json:"type"`
	Record interface{} `json:"rec"`
}

var (
	EvtCreateRecord = "c"
	EvtUpdateRecord = "u"
	EvtDeleteRecord = "d"
)

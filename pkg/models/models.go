package models

import (
	"github.com/goccy/go-json"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
)

type Event struct {
	Did       string                                  `json:"did"`
	TimeUS    int64                                   `json:"time_us"`
	EventType string                                  `json:"type"`
	Commit    *Commit                                 `json:"commit,omitempty"`
	Account   *comatproto.SyncSubscribeRepos_Account  `json:"account,omitempty"`
	Identity  *comatproto.SyncSubscribeRepos_Identity `json:"identity,omitempty"`
}

type Commit struct {
	Rev        string          `json:"rev,omitempty"`
	OpType     string          `json:"type"`
	Collection string          `json:"collection,omitempty"`
	RKey       string          `json:"rkey,omitempty"`
	Record     json.RawMessage `json:"record,omitempty"`
	CID        string          `json:"cid,omitempty"`
}

var (
	EventCommit   = "com"
	EventAccount  = "acc"
	EventIdentity = "id"

	CommitCreateRecord = "c"
	CommitUpdateRecord = "u"
	CommitDeleteRecord = "d"
)

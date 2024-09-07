package models

import (
	"github.com/goccy/go-json"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
)

type Event struct {
	Did       string                                  `json:"did" cborgen:"did"`
	TimeUS    int64                                   `json:"time_us" cborgen:"time_us"`
	EventType string                                  `json:"type" cborgen:"type"`
	Commit    *Commit                                 `json:"commit,omitempty" cborgen:"commit,omitempty"`
	Account   *comatproto.SyncSubscribeRepos_Account  `json:"account,omitempty" cborgen:"account,omitempty"`
	Identity  *comatproto.SyncSubscribeRepos_Identity `json:"identity,omitempty" cborgen:"identity,omitempty"`
}

type Commit struct {
	Rev        string          `json:"rev,omitempty" cborgen:"rev"`
	OpType     string          `json:"type" cborgen:"type"`
	Collection string          `json:"collection,omitempty" cborgen:"collection"`
	RKey       string          `json:"rkey,omitempty" cborgen:"rkey"`
	Record     json.RawMessage `json:"record,omitempty" cborgen:"record,omitempty"`
}

var (
	EventCommit   = "com"
	EventAccount  = "acc"
	EventIdentity = "id"

	CommitCreateRecord = "c"
	CommitUpdateRecord = "u"
	CommitDeleteRecord = "d"
)

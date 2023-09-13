package consumer

import (
	"github.com/bluesky-social/indigo/api/bsky"
)

type Event struct {
	Did    string `json:"did"`
	Seq    int64  `json:"seq"`
	OpType string `json:"opType"`

	RecType string `json:"recType,omitempty"`

	DeleteRef string `json:"deleteRef,omitempty"`

	Handle string `json:"handle,omitempty"`

	Profile *bsky.ActorProfile `json:"profile,omitempty"`
	Post    *bsky.FeedPost     `json:"post,omitempty"`
	Like    *bsky.FeedLike     `json:"like,omitempty"`
	Repost  *bsky.FeedRepost   `json:"repost,omitempty"`
	Follow  *bsky.GraphFollow  `json:"follow,omitempty"`
	Block   *bsky.GraphBlock   `json:"block,omitempty"`

	List          *bsky.GraphList     `json:"list,omitempty"`
	ListItem      *bsky.GraphListitem `json:"listItem,omitempty"`
	FeedGenerator *bsky.FeedGenerator `json:"feedGenerator,omitempty"`
}

var (
	EvtCreateRecord = "c"
	EvtUpdateRecord = "u"
	EvtDeleteRecord = "d"
)

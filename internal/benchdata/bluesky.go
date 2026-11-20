// Package benchdata generates events shaped like the Bluesky firehose that
// JSONBench loads, for benchmarks that must not depend on a dataset file.
package benchdata

import (
	"math"
	"strconv"
	"time"
)

// collections are the commit collections with their share of events in
// parts per thousand, close to the first ten million events of the dataset.
var collections = []struct {
	name  string
	share int
}{
	{"app.bsky.feed.like", 463},
	{"app.bsky.graph.follow", 322},
	{"app.bsky.feed.post", 90},
	{"app.bsky.feed.repost", 58},
	{"app.bsky.graph.block", 14},
	{"app.bsky.graph.listitem", 7},
	{"app.bsky.actor.profile", 6},
	{"app.bsky.graph.listblock", 1},
}

var words = []string{
	"the", "and", "that", "have", "for", "not", "with", "you", "this", "but", "his", "from", "they", "say", "her",
	"she", "will", "one", "all", "would", "there", "their", "what", "out", "about", "who", "get", "which", "when",
	"make", "can", "like", "time", "just", "him", "know", "take", "people", "into", "year", "your", "good", "some",
	"could", "them", "see", "other", "than", "then", "now", "look", "only", "come", "its", "over", "think", "also",
	"back", "after", "use", "two", "how", "our", "work", "first", "well", "way", "even", "new", "want", "because",
	"any", "these", "give", "day", "most", "bluesky", "feed", "post", "thread", "skeet", "timeline", "moderation",
}

const base32 = "abcdefghijklmnopqrstuvwxyz234567"

// sortable is the alphabet of record keys and revisions, in the order of
// its bytes so that the keys sort as the times they hold.
const sortable = "234567abcdefghijklmnopqrstuvwxyz"

// Generator yields the same events for the same seed.
type Generator struct {
	state uint64
	users uint64
	time  int64
}

// New returns a generator of events spread over users accounts; active
// accounts write far more than the rest, as on the network.
func New(seed uint64, users int) *Generator {
	return &Generator{state: seed*0x9e3779b97f4a7c15 + 1, users: uint64(max(users, 1)), time: 1732206349000167}
}

func (g *Generator) next() uint64 {
	g.state += 0x9e3779b97f4a7c15
	z := g.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb

	return z ^ (z >> 31)
}

func (g *Generator) id(dst []byte, n int, v uint64) []byte {
	for i := 0; i < n; i++ {
		if i%12 == 11 {
			v = v*0x9e3779b97f4a7c15 + uint64(i)
		}
		dst = append(dst, base32[v&31])
		v = v>>5 | v<<59
	}

	return dst
}

func (g *Generator) did(dst []byte) []byte {
	return g.appendDID(dst, g.account())
}

// account draws an account; the square of a uniform draw favours the low
// account numbers.
func (g *Generator) account() uint64 {
	f := float64(g.next()>>11) / (1 << 53)

	return uint64(f * f * float64(g.users))
}

func (g *Generator) appendDID(dst []byte, account uint64) []byte {
	dst = append(dst, "did:plc:"...)

	return g.id(dst, 24, account*0xd6e8feb86659fd93+0x2545f4914f6cdd1d)
}

// appendTID appends the 13 characters that a microsecond time and a clock
// identifier of 10 bits make, the time in the high bits.
func appendTID(dst []byte, us int64, clock uint64) []byte {
	v := uint64(us)<<10 | clock&1023
	var buf [13]byte
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = sortable[v&31]
		v >>= 5
	}

	return append(dst, buf[:]...)
}

// clock returns the clock identifier that an account mints keys of one kind
// with.
func clock(account, kind uint64) uint64 {
	return (account*0x9e3779b97f4a7c15 + kind*0xbf58476d1ce4e5b9) >> 54
}

// age draws how long before now a record was made, between a second and a
// month, as many of each order of magnitude as of any other.
func (g *Generator) age() int64 {
	const least, most = 1e6, 30 * 24 * 3600 * 1e6
	f := float64(g.next()>>11) / (1 << 53)

	return int64(least * math.Exp(f*math.Log(most/least)))
}

func (g *Generator) text(dst []byte) []byte {
	n := 3 + int(g.next()%28)
	for i := 0; i < n; i++ {
		if i > 0 {
			dst = append(dst, ' ')
		}
		dst = append(dst, words[g.next()%uint64(len(words))]...)
	}

	return dst
}

// Append adds n events to dst, one JSON object per line.
func (g *Generator) Append(dst []byte, n int) []byte {
	for ; n > 0; n-- {
		g.time += int64(g.next() % 400)
		r := g.next()
		author := g.account()
		dst = append(dst, `{"did":"`...)
		dst = g.appendDID(dst, author)
		dst = append(dst, `","time_us":`...)
		dst = strconv.AppendInt(dst, g.time, 10)
		if r%100 == 0 {
			dst = append(dst, `,"kind":"identity","identity":{"did":"`...)
			dst = g.did(dst)
			dst = append(dst, `","handle":"`...)
			dst = g.id(dst, 9, g.next())
			dst = append(dst, `.bsky.social","seq":`...)
			dst = strconv.AppendUint(dst, g.next()%4000000000, 10)
			dst = append(dst, "}}\n"...)

			continue
		}
		pick, collection := int(r>>8%1000), collections[len(collections)-1].name
		for _, c := range collections {
			if pick < c.share {
				collection = c.name

				break
			}
			pick -= c.share
		}
		operation := "create"
		switch op := r >> 24 % 100; {
		case op < 4:
			operation = "delete"
		case op < 5:
			operation = "update"
		}
		// A revision is minted with its commit, shortly before the event is
		// seen; a record key when its record is made, a little earlier, and a
		// deleted record was made long before.
		dst = append(dst, `,"kind":"commit","commit":{"rev":"`...)
		dst = appendTID(dst, g.time-int64(g.next()%300000), clock(author, 1))
		dst = append(dst, `","operation":"`...)
		dst = append(dst, operation...)
		dst = append(dst, `","collection":"`...)
		dst = append(dst, collection...)
		dst = append(dst, `","rkey":"`...)
		created := g.time - int64(g.next()%2000000)
		switch {
		case collection == "app.bsky.actor.profile":
			dst = append(dst, "self"...)
		case operation == "delete":
			dst = appendTID(dst, g.time-g.age(), clock(author, 2))
		default:
			dst = appendTID(dst, created, clock(author, 2))
		}
		if operation == "delete" {
			dst = append(dst, "\"}}\n"...)

			continue
		}
		dst = append(dst, `","record":{"$type":"`...)
		dst = append(dst, collection...)
		dst = append(dst, `","createdAt":"`...)
		dst = time.UnixMicro(created).UTC().AppendFormat(dst, "2006-01-02T15:04:05.000Z")
		dst = append(dst, '"')
		switch collection {
		case "app.bsky.feed.post":
			dst = append(dst, `,"langs":["en"],"text":"`...)
			dst = g.text(dst)
			dst = append(dst, '"')
		case "app.bsky.feed.like", "app.bsky.feed.repost":
			dst = append(dst, `,"subject":{"cid":"bafyrei`...)
			dst = g.id(dst, 52, g.next())
			poster := g.account()
			dst = append(dst, `","uri":"at://`...)
			dst = g.appendDID(dst, poster)
			dst = append(dst, `/app.bsky.feed.post/`...)
			dst = appendTID(dst, g.time-g.age(), clock(poster, 2))
			dst = append(dst, `"}`...)
		default:
			dst = append(dst, `,"subject":"`...)
			dst = g.did(dst)
			dst = append(dst, '"')
		}
		dst = append(dst, `},"cid":"bafyrei`...)
		dst = g.id(dst, 52, g.next())
		dst = append(dst, "\"}}\n"...)
	}

	return dst
}

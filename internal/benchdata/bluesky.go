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
	// step is the mean number of microseconds from one event to the next.
	step uint64
}

// New returns a generator of events spread over users accounts; active
// accounts write far more than the rest, as on the network.
func New(seed uint64, users int) *Generator {
	return &Generator{state: seed*0x9e3779b97f4a7c15 + 1, users: uint64(max(users, 1)), time: 1732206349000167, step: 200}
}

// Spread makes the events that follow lie about step microseconds apart, 200
// unless set, so that a few million of them can cover several days.
func (g *Generator) Spread(step int64) *Generator {
	g.step = uint64(max(step, 1))

	return g
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
// with, one of ids.
func clock(account, kind, ids uint64) uint64 {
	return ((account*0x9e3779b97f4a7c15 + kind*0xbf58476d1ce4e5b9) >> 32 % ids) * 0x2f1 & 1023
}

// uniform returns a draw in [0, 1).
func (g *Generator) uniform() float64 { return float64(g.next()>>11) / (1 << 53) }

// normal returns a draw of the standard normal law.
func (g *Generator) normal() float64 {
	return math.Sqrt(-2*math.Log(1-g.uniform())) * math.Cos(2*math.Pi*g.uniform())
}

// between returns a draw whose power of ten is uniform between from and to.
func (g *Generator) between(from, to float64) int64 {
	return int64(math.Pow(10, from+(to-from)*g.uniform()))
}

// age draws how long before now a record was made, between a second and a
// month, as many of each order of magnitude as of any other.
func (g *Generator) age() int64 { return g.between(6, math.Log10(30*24*3600*1e6)) }

// lag draws how long before the event its commit was made, in microseconds:
// a median of 47 s, a tenth under 3 s and a tenth over 20 min, and all but
// a thousandth within an hour, as on the network.
func (g *Generator) lag() int64 {
	return int64(math.Pow(10, min(max(7.67+1.05*g.normal(), 4.5), 9.55)))
}

// skew draws how long before its commit the client made a record key, in
// microseconds: a second or two, a few minutes for one client in fifty.
func (g *Generator) skew() int64 {
	if g.next()%50 == 0 {
		return g.between(6, 8.5)
	}

	return int64(math.Pow(10, 6.2+0.5*g.normal()))
}

// deletedAge draws the age of a record that a delete names, in
// microseconds: under an hour for three in ten, under a day for half, and
// over a year for one in a hundred.
func (g *Generator) deletedAge() int64 {
	switch u := g.uniform(); {
	case u < 0.30:
		return g.between(4, 9.56)
	case u < 0.53:
		return g.between(9.56, 10.94)
	case u < 0.99:
		return g.between(10.94, 13.2)
	default:
		return g.between(13.5, 14)
	}
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

// appendCreatedAt appends the time a client says it made a record at, its
// clock's time of the record key: in milliseconds and UTC for all but a few,
// which some clients write without a fraction, with more digits or with an
// offset, and one in a hundred dated long before, as imported records are.
func (g *Generator) appendCreatedAt(dst []byte, us int64, post bool) []byte {
	if g.next()%100 == 0 {
		us -= g.between(13.3, 14.2)
	}
	t := time.UnixMicro(us).UTC()
	switch r := g.next() % 1000; {
	case post && r < 43:
		return t.AppendFormat(dst, "2006-01-02T15:04:05Z")
	case r == 990:
		return t.AppendFormat(dst, "2006-01-02T15:04:05.000000Z")
	case r == 991:
		return t.AppendFormat(dst, "2006-01-02T15:04:05.000+00:00")
	}

	return t.AppendFormat(dst, "2006-01-02T15:04:05.000Z")
}

// Append adds n events to dst, one JSON object per line.
func (g *Generator) Append(dst []byte, n int) []byte {
	for ; n > 0; n-- {
		g.time += int64(g.next() % (2 * g.step))
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
		// A revision is minted with its commit by the server, a record key a
		// little before by the client that makes the record, with its time
		// of creation; a deleted record was made long before. A few keys
		// are names or of other alphabets.
		dst = append(dst, `,"kind":"commit","commit":{"rev":"`...)
		committed := g.time - g.lag()
		dst = appendTID(dst, committed, clock(author, 1, 32))
		dst = append(dst, `","operation":"`...)
		dst = append(dst, operation...)
		dst = append(dst, `","collection":"`...)
		dst = append(dst, collection...)
		dst = append(dst, `","rkey":"`...)
		created := committed - g.skew()
		switch named := g.next() % 1000; {
		case collection == "app.bsky.actor.profile", operation == "update" && named < 938, named < 5:
			dst = append(dst, "self"...)
		case named == 5:
			dst = g.id(dst, 9, g.next())
			dst = strconv.AppendUint(dst, 1000+g.next()%9000, 10)
		case operation == "delete":
			dst = appendTID(dst, g.time-g.deletedAge(), clock(author, 2, 63))
		default:
			dst = appendTID(dst, created, clock(author, 2, 63))
		}
		if operation == "delete" {
			dst = append(dst, "\"}}\n"...)

			continue
		}
		dst = append(dst, `","record":{"$type":"`...)
		dst = append(dst, collection...)
		dst = append(dst, `","createdAt":"`...)
		dst = g.appendCreatedAt(dst, created, collection == "app.bsky.feed.post")
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
			dst = appendTID(dst, g.time-g.age(), clock(poster, 2, 63))
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

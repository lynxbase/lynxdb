// Package benchdata generates the data of benchmarks and tests that must
// not depend on a dataset file: events shaped like the Bluesky firehose that
// JSONBench loads, and application logs as JSON and as text.
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
	// favour is the power of the uniform draw that picks an account.
	favour int
	// batch is the commit of several records whose events come next.
	batch batch
	// rare is a word that one post in rareIn ends with, when rareIn is set.
	rare   string
	rareIn uint64
	// posts is the number of latest posts that likes, reposts and replies
	// name, zero when each names a post of its own; made counts the posts
	// written so far.
	posts, made uint64
}

// batch is a commit of several records of one account, collection and
// operation, whose events follow one another.
type batch struct {
	left                  int
	author                uint64
	collection, operation string
	committed, created    int64
	// jitter is the offset of the records' createdAt from their keys' time,
	// and sameTime tells that the records share their time of creation.
	jitter   int64
	sameTime bool
}

// New returns a generator of events spread over users accounts; active
// accounts write far more than the rest, as on the network.
func New(seed uint64, users int) *Generator {
	return &Generator{state: seed*0x9e3779b97f4a7c15 + 1, users: uint64(max(users, 1)), time: 1732206349000167, step: 200, favour: 2}
}

// Favour makes the events that follow draw their account as a power of a
// uniform draw, 2 unless set: the higher the power, the more of the events
// the most active accounts write, as bots do on the network.
func (g *Generator) Favour(power int) *Generator {
	g.favour = max(power, 1)

	return g
}

// Spread makes the events that follow lie about step microseconds apart, 200
// unless set, so that a few million of them can cover several days.
func (g *Generator) Spread(step int64) *Generator {
	g.step = uint64(max(step, 1))

	return g
}

// Rare makes one in oneIn of the posts that follow end with word, which the
// other texts do not hold, so that a search for it keeps a few rows of a
// block.
func (g *Generator) Rare(word string, oneIn int) *Generator {
	g.rare, g.rareIn = word, uint64(max(oneIn, 1))

	return g
}

// Posts makes the likes and reposts that follow name one of the latest pool
// posts, the newer the likelier, and three posts in ten a reply to one of
// them, so that many events name the same post as on the network; unless
// set, every like names a post of its own and no post is a reply.
func (g *Generator) Posts(pool int) *Generator {
	g.posts = uint64(max(pool, 0))

	return g
}

// appendPost appends the cid and uri members of a reference to one of the
// latest posts, drawn anew at every call.
func (g *Generator) appendPost(dst []byte) []byte {
	f := g.uniform()
	post := (g.made - uint64(f*f*f*float64(g.posts))) * 0x9e3779b97f4a7c15
	poster := post >> 20 % g.users
	dst = append(dst, `"cid":"bafyrei`...)
	dst = g.id(dst, 52, post)
	dst = append(dst, `","uri":"at://`...)
	dst = g.appendDID(dst, poster)
	dst = append(dst, `/app.bsky.feed.post/`...)
	dst = appendTID(dst, int64(1732206349000167-post>>44), clock(poster, 2, 63))

	return append(dst, '"')
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

// account draws an account; a power of a uniform draw favours the low
// account numbers.
func (g *Generator) account() uint64 {
	f := float64(g.next()>>11) / (1 << 53)
	p := f
	for range g.favour - 1 {
		p *= f
	}

	return uint64(p * float64(g.users))
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
// a thousandth within an hour, as on the network; but one event in five
// comes from a delivery late by one to forty hours, as after an outage.
func (g *Generator) lag() int64 {
	if g.next()%5 == 0 {
		return g.between(9.55, 11.15)
	}

	return int64(math.Pow(10, min(max(7.67+1.05*g.normal(), 4.5), 9.55)))
}

// skew draws how long before its commit the client made a record key, in
// microseconds: a second or two, under a few minutes for all but a few, and
// up to days for one client in two thousand.
func (g *Generator) skew() int64 {
	if g.next()%2000 == 0 {
		return g.between(8, 11.4)
	}

	return int64(math.Pow(10, 6.2+0.8*g.normal()))
}

// jitter draws how far, in microseconds, a client's createdAt lies from the
// time of its record key: tens of milliseconds, a few seconds at most for
// all but a few, either way.
func (g *Generator) jitter() int64 {
	j := int64(math.Pow(10, 4.5+0.7*g.normal()))
	if g.next()%2 == 0 {
		return -j
	}

	return j
}

// batchOdds is one in how many commits holds several records; their events
// make about a sixth of the revisions repeat the one before, as on the
// network.
const batchOdds = 14

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
	if g.rareIn > 0 && g.next()%g.rareIn == 0 {
		dst = append(append(dst, ' '), g.rare...)
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
		member := g.batch.left > 0
		if member {
			author = g.batch.author
			g.batch.left--
		}
		dst = append(dst, `{"did":"`...)
		dst = g.appendDID(dst, author)
		dst = append(dst, `","time_us":`...)
		dst = strconv.AppendInt(dst, g.time, 10)
		if !member && r%100 == 0 {
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
		committed, created, jitter := g.time-g.lag(), int64(0), g.jitter()
		created = committed - g.skew()
		switch {
		case member:
			collection, operation, committed, jitter = g.batch.collection, g.batch.operation, g.batch.committed, g.batch.jitter
			created = committed - g.skew()
			if g.batch.sameTime {
				// Records made at once take keys a few microseconds apart.
				g.batch.created += 1 + int64(g.next()%8)
				created = g.batch.created
			}
		case collection != "app.bsky.actor.profile" && g.next()%batchOdds == 0:
			g.batch = batch{
				left: 1 + int(g.next()%5), author: author, collection: collection, operation: operation,
				committed: committed, created: created, jitter: jitter, sameTime: g.next()%2 == 0,
			}
		}
		// A revision is minted with its commit by the server, a record key a
		// little before by the client that makes the record, with its time
		// of creation; a deleted record was made long before. A few keys
		// are names or of other alphabets.
		dst = append(dst, `,"kind":"commit","commit":{"rev":"`...)
		dst = appendTID(dst, committed, clock(author, 1, 32))
		dst = append(dst, `","operation":"`...)
		dst = append(dst, operation...)
		dst = append(dst, `","collection":"`...)
		dst = append(dst, collection...)
		dst = append(dst, `","rkey":"`...)
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
		dst = g.appendCreatedAt(dst, created+jitter, collection == "app.bsky.feed.post")
		dst = append(dst, '"')
		switch collection {
		case "app.bsky.feed.post":
			dst = append(dst, `,"langs":["en"],"text":"`...)
			dst = g.text(dst)
			dst = append(dst, '"')
			if g.posts > 0 {
				g.made++
				if g.next()%10 < 3 {
					dst = append(dst, `,"reply":{"parent":{`...)
					dst = g.appendPost(dst)
					dst = append(dst, `},"root":{`...)
					dst = g.appendPost(dst)
					dst = append(dst, `}}`...)
				}
			}
		case "app.bsky.feed.like", "app.bsky.feed.repost":
			if g.posts > 0 {
				dst = append(dst, `,"subject":{`...)
				dst = g.appendPost(dst)
				dst = append(dst, '}')

				break
			}
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

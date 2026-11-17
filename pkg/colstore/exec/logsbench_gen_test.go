package exec_test

import (
	"bufio"
	"io"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
)

// A generator of application logs: JSON events of fifty services in four
// logger styles, mixed with access log and syslog lines that are not JSON.

const (
	logServiceCount = 50
	logUserCount    = 1_000_000
	logRareKeys     = 2400
	logNodeCount    = 400
)

// logsEpoch is the arrival time of the first line.
var logsEpoch = time.Date(2026, time.March, 2, 6, 0, 0, 0, time.UTC)

// logGenOptions sizes a generated stream.
type logGenOptions struct {
	Seed   uint64
	Events int
	// Span is the time over which the lines arrive.
	Span time.Duration
	// Text is the share of lines that are plain text.
	Text float64
	// WrapText writes a text line as an object that holds the line in a
	// _raw member and its time in a ts member, for stores that take only
	// JSON.
	WrapText bool
}

const (
	logDebug = iota
	logInfo
	logWarn
	logError
	logFatal
)

const (
	styleGo = iota
	styleJava
	stylePython
	styleNode
)

var logLevelNames = [2][5]string{
	{"debug", "info", "warn", "error", "fatal"},
	{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"},
}

type logField uint8

const (
	fTS logField = iota
	fLevel
	fService
	fEnv
	fHost
	fPod
	fVersion
	fLogger
	fThread
	fCaller
	fLine
	fPID
	fTrace
	fSpan
	fUser
	fMessage
	fHTTP
	fDuration
	fError
	fCode
	fTags
	fExtra
)

// logFieldOrder is the member order of each logger style.
var logFieldOrder = [4][]logField{
	styleGo:     {fTS, fLevel, fService, fEnv, fHost, fPod, fVersion, fCaller, fTrace, fSpan, fUser, fMessage, fHTTP, fDuration, fError, fCode, fTags, fExtra},
	styleJava:   {fTS, fLevel, fLogger, fThread, fService, fEnv, fHost, fPod, fVersion, fTrace, fSpan, fMessage, fUser, fHTTP, fDuration, fError, fCode, fTags, fExtra},
	stylePython: {fLevel, fTS, fService, fEnv, fHost, fPod, fLogger, fLine, fMessage, fTrace, fSpan, fUser, fHTTP, fDuration, fCode, fError, fTags, fExtra},
	styleNode:   {fLevel, fTS, fPID, fHost, fService, fEnv, fPod, fVersion, fTrace, fSpan, fUser, fMessage, fHTTP, fError, fDuration, fCode, fExtra, fTags},
}

var (
	logNouns = []string{"checkout", "payments", "auth", "catalog", "search", "cart", "inventory", "shipping", "billing", "notifications",
		"profile", "recommendations", "pricing", "orders", "gateway", "sessions", "media", "ledger", "fraud", "reports",
		"scheduler", "emails", "webhooks", "identity", "geo"}
	logRoles     = []string{"api", "worker", "svc", "consumer"}
	logResources = []string{"users", "orders", "carts", "products", "payments", "invoices", "sessions", "shipments", "reviews", "accounts",
		"coupons", "addresses", "subscriptions", "refunds", "tickets", "devices", "teams", "projects", "files", "events"}
	logSubs       = []string{"items", "orders", "history", "settings", "tokens", "comments", "attachments", "status", "preferences", "members"}
	logComponents = []string{"pool", "cache", "db", "kafka", "http", "grpc", "scheduler", "auth", "storage", "indexer", "router", "limiter",
		"billing", "mailer", "exporter", "resolver", "session", "queue", "replicator", "gc", "loader", "tracer", "config", "lock"}
	logAdjectives = []string{"fast", "new", "legacy", "beta", "canary", "shadow", "async", "strict", "lazy", "bulk", "regional", "cached",
		"signed", "partial", "inline", "remote", "local", "dynamic", "static", "primary", "secondary", "experimental", "safe", "dark",
		"early", "late", "soft", "hard", "smart", "raw"}
	logKeyNouns = []string{"checkout", "pricing", "ranking", "retry", "timeout", "batch", "cursor", "shard", "tenant", "region", "quota",
		"bucket", "cohort", "variant", "window", "budget", "offset", "replica", "partition", "segment", "fanout", "backoff", "threshold",
		"sample", "attempt", "depth", "width", "weight", "score", "limit", "ttl", "epoch", "checksum", "version", "revision", "locale",
		"currency", "channel", "campaign", "device"}
	logKeySuffixes = []string{"", "_v2", "_ms", "_count"}
	logNamespaces  = []string{"flags", "labels", "params", "metrics", "ctx", "k8s"}
	logTags        = []string{"canary", "batch", "eu", "us", "apac", "internal", "partner", "mobile", "web", "retry", "replay", "synthetic",
		"priority", "bulk", "beta", "pci", "gdpr", "cold-start", "cached", "shadow"}
	logMethods = []string{"GET", "GET", "GET", "GET", "GET", "GET", "GET", "POST", "POST", "PUT", "DELETE", "PATCH"}
	logPorts   = []int{5432, 6379, 9092, 8080, 443, 3306, 9200, 27017, 50051}
	logAgents  = []string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
		"Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
		"Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.39 Mobile Safari/537.36",
		"okhttp/4.12.0", "python-requests/2.32.3", "Go-http-client/2.0", "curl/8.7.1", "kube-probe/1.31", "Prometheus/2.55.0",
		"axios/1.7.7", "Apache-HttpClient/5.4 (Java/21.0.4)", "grpc-go/1.68.0", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
	}
	logQuoted = []string{
		"SELECT id, status, total FROM orders WHERE user_id = $1 ORDER BY created_at DESC LIMIT 50",
		"UPDATE inventory SET reserved = reserved + $1 WHERE sku = $2 AND available >= $1",
		"INSERT INTO audit_log (actor, action, target, at) VALUES ($1, $2, $3, now())",
		"SELECT * FROM sessions WHERE token = $1 AND expires_at > now()",
		"DELETE FROM carts WHERE updated_at < now() - interval '30 days'",
		"orders.created", "payments.settled", "users.signup", "inventory.low-stock", "emails.outbound",
		"insufficient funds", "card expired", "do not honor", "nightly-reconcile", "rebuild-search-index", "page_size", "include_deleted",
	}
	logErrorTypes = [4][]string{
		styleGo:     {"context.DeadlineExceeded", "*net.OpError", "*pq.Error", "io.ErrUnexpectedEOF", "*url.Error", "redis.Nil", "*json.SyntaxError", "sql.ErrTxDone"},
		styleJava:   {"java.lang.NullPointerException", "java.lang.IllegalStateException", "java.net.SocketTimeoutException", "java.sql.SQLTransientConnectionException", "org.springframework.dao.DeadlockLoserDataAccessException", "com.fasterxml.jackson.core.JsonParseException", "java.util.concurrent.TimeoutException", "java.lang.OutOfMemoryError", "org.apache.kafka.common.errors.TimeoutException"},
		stylePython: {"ValueError", "KeyError", "TimeoutError", "ConnectionResetError", "psycopg2.OperationalError", "redis.exceptions.ConnectionError", "json.decoder.JSONDecodeError", "PermissionError"},
		styleNode:   {"TypeError", "RangeError", "Error", "SyntaxError", "ECONNRESET", "ETIMEDOUT", "ValidationError", "SequelizeConnectionError"},
	}
	logErrorMessages = []string{"connection reset by peer", "deadline exceeded", "connection refused", "unexpected end of input",
		"deadlock detected", "no space left on device", "too many open files", "permission denied", "broken pipe", "value out of range"}
)

// logFamilies are the message patterns of each level class; a template is a
// pattern prefixed with a component name.
var logFamilies = [4][]string{
	logDebug: {
		"cache hit for key {key}",
		"cache miss for key {key}, loading from store",
		"acquired lock {key} in {ms}ms",
		"serialized {n} records ({bytes} bytes) in {ms}ms",
		"executing query {q} with {n} parameters",
		"connection {hex} checked out from pool (active={n}, idle={n})",
		"received message {uuid} from partition {n} at offset {id}",
		"resolved {host} to {ip} in {ms}ms",
		"flag {word} evaluated to {bool} for user {id}",
		"opening file {path}",
		"heartbeat sent to {host}:{port}",
		"parsed {n} headers from request {hex}",
	},
	logInfo: {
		"started worker {n} on {host}",
		"processed batch {id} of {n} items in {ms}ms",
		"user {id} logged in from {ip}",
		"order {id} created for user {id} with {n} items",
		"payment {uuid} authorized for {money}",
		"published event {word} to topic {q}",
		"wrote {bytes} bytes to {path}",
		"compaction of {path} finished in {ms}ms",
		"scheduled job {q} to run in {n}s",
		"session {hex} expired after {n} minutes",
		"email {uuid} queued for delivery to {email}",
		"config reloaded from {path}",
		"upstream {host}:{port} marked healthy after {n} checks",
		"snapshot {hex} uploaded ({bytes} bytes)",
		"rate limiter refilled bucket {key} to {n} tokens",
	},
	logWarn: {
		"slow query took {ms}ms: {q}",
		"retrying request {hex} (attempt {n} of {n})",
		"connection pool nearly exhausted (active={n}, idle={n}, waiting={n})",
		"response from {host}:{port} took {ms}ms, exceeding the {n}ms budget",
		"rate limit exceeded for client {ip} on {route}",
		"deprecated parameter {q} used by client {hex}",
		"disk usage of {path} at {n}%",
		"consumer lag of {id} messages on partition {n}",
		"token for user {id} expires in {n}s",
		"dropped {n} spans, exporter queue full",
		"gc pause of {ms}ms",
	},
	logError: {
		"failed to connect to {host}:{port}: connection refused",
		"request {hex} failed: connection reset by peer",
		"timeout after {n}ms waiting for {host}:{port}",
		"failed to write {path}: no space left on device",
		"deadlock detected while updating order {id}, transaction rolled back",
		"payment {uuid} declined: {q}",
		"unhandled exception while processing message {uuid}",
		"could not parse payload of {bytes} bytes: unexpected token at offset {n}",
		"upstream {host}:{port} returned status {n}",
		"failed to acquire lock {key} after {ms}ms",
		"permission denied for user {id} on resource {path}",
		"circuit breaker opened for {host} after {n} consecutive failures",
		"out of memory: container exceeded limit of {n}Mi",
	},
}

var logSyslogFamilies = []string{
	"sshd[{n}]: Failed password for invalid user {word} from {ip} port {port} ssh2",
	"sshd[{n}]: Accepted publickey for deploy from {ip} port {port} ssh2: ED25519 SHA256:{hex}{hex}",
	"sshd[{n}]: Connection closed by authenticating user root {ip} port {port} [preauth]",
	"kernel: [{ms}] TCP: request_sock_TCP: Possible SYN flooding on port {port}. Sending cookies.",
	"kernel: [{ms}] oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),cpuset={hex},mems_allowed=0,oom_memcg=/kubepods/burstable/pod{uuid}",
	"kernel: [{ms}] EXT4-fs (nvme0n1p1): mounted filesystem with ordered data mode. Opts: (null)",
	"kernel: [{ms}] IPv4: martian source {ip} from {ip}, on dev eth0",
	"systemd[1]: Started Session {n} of user {word}.",
	"systemd[1]: {word}.service: Main process exited, code=exited, status={n}/FAILURE",
	"systemd[1]: {word}.service: Scheduled restart job, restart counter is at {n}.",
	"CRON[{n}]: ({word}) CMD ({path} --quiet)",
	"kubelet[{n}]: I0302 {ms} {n} kubelet.go:{n}] \"SyncLoop (PLEG): event for pod\" pod=\"prod/{host}\" event={hex}",
	"kubelet[{n}]: E0302 {ms} {n} pod_workers.go:{n}] \"Error syncing pod, skipping\" err=\"failed to StartContainer: back-off {n}s restarting failed container\" pod=\"prod/{host}\"",
	"containerd[{n}]: time=\"{ms}\" level=info msg=\"shim disconnected\" id={hex}{hex}",
	"dhclient[{n}]: DHCPREQUEST on eth0 to {ip} port 67",
	"sudo: {word} : TTY=pts/{n} ; PWD={path} ; USER=root ; COMMAND=/usr/bin/systemctl restart {word}",
	"chronyd[{n}]: Selected source {ip}",
	"dockerd[{n}]: level=warning msg=\"container {hex} exceeded memory limit\"",
	"postfix/smtpd[{n}]: connect from unknown[{ip}]",
	"haproxy[{n}]: {ip}:{port} [{ms}] frontend backend/{host} {n}/{n}/{n}/{n}/{n} {n} {bytes} - - ---- {n}/{n}/{n}/{n}/{n} {n}/{n} \"GET {route} HTTP/1.1\"",
}

type logSlot uint8

const (
	slotN logSlot = iota
	slotID
	slotMS
	slotBytes
	slotHex
	slotUUID
	slotKey
	slotQuoted
	slotPath
	slotHost
	slotPort
	slotIP
	slotEmail
	slotMoney
	slotWord
	slotBool
	slotRoute
)

var logSlotNames = map[string]logSlot{"n": slotN, "id": slotID, "ms": slotMS, "bytes": slotBytes, "hex": slotHex, "uuid": slotUUID,
	"key": slotKey, "q": slotQuoted, "path": slotPath, "host": slotHost, "port": slotPort, "ip": slotIP, "email": slotEmail,
	"money": slotMoney, "word": slotWord, "bool": slotBool, "route": slotRoute}

// logTemplate is a message: literals with a slot between each two.
type logTemplate struct {
	lits  []string
	slots []logSlot
}

func parseLogTemplate(text string) logTemplate {
	var t logTemplate
	for {
		open := strings.IndexByte(text, '{')
		if open < 0 {
			t.lits = append(t.lits, text)

			return t
		}
		end := open + strings.IndexByte(text[open:], '}')
		t.lits = append(t.lits, text[:open])
		t.slots = append(t.slots, logSlotNames[text[open+1:end]])
		text = text[end+1:]
	}
}

const (
	keyBool = iota
	keyLabel
	keyInt
	keyFloat
	keyID
	keyMixed
)

// logKey is a rare member, nested in an object per namespace and, in one
// namespace, per group.
type logKey struct {
	ns, group, name string
	order           int
	kind            int
	labels          []string
}

func (k *logKey) path() string {
	if k.group != "" {
		return k.ns + "." + k.group + "." + k.name
	}

	return k.ns + "." + k.name
}

type logPod struct{ name, host, env string }

type logService struct {
	name, noun string
	style      int
	// versions and pods are those before and after the service's deploy,
	// which happens deployAt of the way through the span.
	versions [2]string
	pods     [2][]logPod
	deployAt float64
	// levels are the cumulative shares of the levels below fatal.
	levels [4]float64
	// incident is the part of the span in which the service fails often.
	incident  [2]float64
	routes    []int
	templates [4][]int
	keys      []int
	keyZipf   *rand.Zipf
	frames    []string
	loggers   []string
	callers   []string
	codeKind  int
	uuids     bool
	latency   float64
}

// logGen writes the lines of a stream, the same ones for the same options.
type logGen struct {
	opts logGenOptions
	r    *rand.Rand
	// clock is the arrival time of the last line and step the mean time
	// between two lines, in microseconds since logsEpoch.
	clock, step float64

	services  []logService
	svcZipf   *rand.Zipf
	userZipf  *rand.Zipf
	pickZipf  *rand.Zipf
	templates []logTemplate
	syslog    []logTemplate
	routes    []string
	keys      []logKey
	global    []int
	nodes     []string
	edges     []string
	// shared are the framework frames under the frames of a service.
	shared [4][]string

	// burst is a backlog that one pod ships long after its events.
	burst struct {
		left int
		at   float64
		svc  *logService
		pod  *logPod
	}
	drawn []int
	path  []byte
	text  []byte
}

const hexDigits = "0123456789abcdef"

func (g *logGen) hex(dst []byte, n int) []byte {
	for n > 0 {
		v := g.r.Uint64()
		for i := 0; i < 16 && n > 0; i, n = i+1, n-1 {
			dst = append(dst, hexDigits[v&15])
			v >>= 4
		}
	}

	return dst
}

func (g *logGen) uuid(dst []byte) []byte {
	for i, n := range [...]int{8, 4, 4, 4, 12} {
		if i > 0 {
			dst = append(dst, '-')
		}
		dst = g.hex(dst, n)
	}

	return dst
}

func (g *logGen) ip(dst []byte) []byte {
	r := g.r
	first := 10
	if r.IntN(3) > 0 {
		first = 11 + r.IntN(212)
	}
	dst = strconv.AppendInt(dst, int64(first), 10)
	for range 3 {
		dst = append(dst, '.')
		dst = strconv.AppendInt(dst, int64(r.IntN(256)), 10)
	}

	return dst
}

func (g *logGen) suffix(n int) string {
	const letters = "bcdfghjklmnpqrstvwxz2456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[g.r.IntN(len(letters))]
	}

	return string(b)
}

func pick[T any](r *rand.Rand, list []T) T { return list[r.IntN(len(list))] }

// skewed picks from list, the first elements far more often than the last.
func (g *logGen) skewed(list []int) int { return list[int(g.pickZipf.Uint64())%len(list)] }

func newLogGen(opts logGenOptions) *logGen {
	g := &logGen{opts: opts, r: rand.New(rand.NewPCG(opts.Seed, 0x6c6f6773))}
	r := g.r
	g.step = float64(opts.Span.Microseconds()) / float64(max(opts.Events, 1))
	g.svcZipf = rand.NewZipf(r, 1.25, 2.5, logServiceCount-1)
	g.userZipf = rand.NewZipf(r, 1.07, 12, logUserCount-1)
	g.pickZipf = rand.NewZipf(r, 1.3, 1.5, 63)

	for _, res := range logResources {
		base := "/api/v1/" + res
		g.routes = append(g.routes, base, base+"/{id}", "/internal/"+res+"/sync")
		for _, sub := range logSubs[r.IntN(4):][:4] {
			g.routes = append(g.routes, base+"/{id}/"+sub)
		}
		for _, sub := range logSubs[r.IntN(6):][:2] {
			g.routes = append(g.routes, "/api/v2/"+res+"/{id}/"+sub+"/{id}")
		}
	}
	g.routes = append(g.routes, "/healthz", "/readyz", "/metrics")
	for range logNodeCount {
		g.nodes = append(g.nodes, "ip-10-"+strconv.Itoa(r.IntN(64))+"-"+strconv.Itoa(r.IntN(256))+"-"+strconv.Itoa(r.IntN(256))+".eu-west-1.compute.internal")
	}
	for i := range 6 {
		g.edges = append(g.edges, "edge-"+strconv.Itoa(i+1)+"-"+g.suffix(4))
	}
	var byLevel [4][]int
	for level, families := range logFamilies {
		for f, family := range families {
			for j := range 6 {
				byLevel[level] = append(byLevel[level], len(g.templates))
				g.templates = append(g.templates, parseLogTemplate(logComponents[(f*7+j*5)%len(logComponents)]+": "+family))
			}
		}
	}
	for _, family := range logSyslogFamilies {
		g.syslog = append(g.syslog, parseLogTemplate(family))
	}
	g.shared = [4][]string{
		styleGo:     g.frames(styleGo, "net/http", 30),
		styleJava:   g.frames(styleJava, "org.springframework.web", 90),
		stylePython: g.frames(stylePython, "site-packages/django", 30),
		styleNode:   g.frames(styleNode, "node_modules/express/lib", 40),
	}

	names := r.Perm(logServiceCount)
	g.services = make([]logService, logServiceCount)
	for rank := range g.services {
		s := &g.services[rank]
		s.noun = logNouns[names[rank]%len(logNouns)]
		s.name = s.noun + "-" + logRoles[(names[rank]/len(logNouns)+names[rank])%len(logRoles)]
		s.style = [...]int{styleGo, styleGo, styleGo, styleJava, styleJava, styleJava, stylePython, stylePython, styleNode, styleNode}[r.IntN(10)]
		minor := 1 + r.IntN(80)
		s.versions = [2]string{"1." + strconv.Itoa(minor) + "." + strconv.Itoa(r.IntN(20)), "1." + strconv.Itoa(minor+1) + ".0"}
		s.deployAt = 2
		if rank < 8 {
			s.deployAt = 0.3 + 0.4*r.Float64()
		}
		replicas := 2 + int(120/math.Pow(float64(rank+1), 0.7)) + r.IntN(4)
		for i := range s.pods {
			set := s.name + "-" + g.suffix(9+r.IntN(2))
			for range replicas {
				env := "prod"
				switch u := r.IntN(100); {
				case u < 5:
					env = "dev"
				case u < 20:
					env = "staging"
				}
				s.pods[i] = append(s.pods[i], logPod{name: set + "-" + g.suffix(5), host: pick(r, g.nodes), env: env})
			}
		}
		debug := 0.05 + 0.35*r.Float64()
		warn := 0.03 + 0.09*r.Float64()
		failing := 0.005 + 0.055*r.Float64()*r.Float64()
		s.levels = [4]float64{debug, 1 - warn - failing - 0.001, 1 - failing - 0.001, 1 - 0.001}
		if rank == 1 || rank == 4 || rank == 9 {
			from := 0.2 + 0.6*r.Float64()
			s.incident = [2]float64{from, from + float64(12*time.Minute)/float64(opts.Span)}
		}
		for range 3 + r.IntN(12) {
			s.routes = append(s.routes, r.IntN(len(g.routes)))
		}
		for level, n := range [...]int{8, 12, 6, 6} {
			for range n {
				s.templates[level] = append(s.templates[level], pick(r, byLevel[level]))
			}
		}
		s.frames = g.frames(s.style, s.noun, 40+r.IntN(40))
		for range 8 {
			switch s.style {
			case styleJava:
				s.loggers = append(s.loggers, "com.acme."+s.noun+"."+title(pick(r, logKeyNouns))+title(pick(r, logComponents))+"Service")
			default:
				s.loggers = append(s.loggers, s.noun+"."+pick(r, logComponents)+"."+pick(r, logKeyNouns))
			}
		}
		for range 20 {
			s.callers = append(s.callers, pick(r, logComponents)+"/"+pick(r, logKeyNouns)+".go:"+strconv.Itoa(20+r.IntN(900)))
		}
		s.codeKind = r.IntN(3)
		s.uuids = r.IntN(7) == 0
		s.latency = 18 * math.Exp(r.NormFloat64()*0.8)
	}

	owner := rand.NewZipf(r, 1.1, 4, logServiceCount-1)
	g.keys = make([]logKey, logRareKeys)
	for i := range g.keys {
		k := &g.keys[i]
		ns := [...]int{0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5}[r.IntN(20)]
		k.ns = logNamespaces[ns]
		k.name = logAdjectives[i%len(logAdjectives)] + "_" + logKeyNouns[i/len(logAdjectives)%len(logKeyNouns)] +
			logKeySuffixes[i/(len(logAdjectives)*len(logKeyNouns))%len(logKeySuffixes)]
		k.order = ns << 8
		switch k.ns {
		case "flags":
			k.kind = keyBool
		case "labels", "k8s":
			k.kind = keyLabel
		case "params":
			k.kind = [...]int{keyInt, keyInt, keyInt, keyInt, keyInt, keyID, keyID, keyID, keyMixed, keyMixed}[r.IntN(10)]
		case "metrics":
			k.kind = [...]int{keyFloat, keyFloat, keyInt}[r.IntN(3)]
		case "ctx":
			group := r.IntN(12)
			k.group, k.order = logComponents[group], k.order|group
			k.kind = [...]int{keyInt, keyLabel}[r.IntN(2)]
		}
		for range 3 + r.IntN(5) {
			k.labels = append(k.labels, pick(r, logAdjectives)+"-"+pick(r, logKeyNouns))
		}
		if r.IntN(100) < 8 {
			g.global = append(g.global, i)

			continue
		}
		s := &g.services[owner.Uint64()]
		s.keys = append(s.keys, i)
	}
	for i := range g.services {
		if s := &g.services[i]; len(s.keys) > 1 {
			s.keyZipf = rand.NewZipf(r, 1.8, 2, uint64(len(s.keys)-1))
		}
	}

	return g
}

func title(s string) string { return strings.ToUpper(s[:1]) + s[1:] }

// frames returns n stack frames of a logger style as they stand inside a
// JSON string, each ending its line.
func (g *logGen) frames(style int, pkg string, n int) []string {
	r := g.r
	out := make([]string, n)
	for i := range out {
		typ, fn := title(pick(r, logKeyNouns))+title(pick(r, logComponents)), pick(r, logAdjectives)+title(pick(r, logKeyNouns))
		line := strconv.Itoa(12 + r.IntN(1200))
		switch style {
		case styleJava:
			if !strings.Contains(pkg, ".") {
				pkg = "com.acme." + pkg
			}
			out[i] = `\tat ` + pkg + "." + pick(r, logComponents) + "." + typ + "." + fn + "(" + typ + ".java:" + line + `)\n`
		case stylePython:
			out[i] = `  File \"/app/` + pkg + "/" + pick(r, logComponents) + "/" + strings.ToLower(typ) + `.py\", line ` + line + ", in " + pick(r, logAdjectives) + "_" + pick(r, logKeyNouns) +
				`\n    ` + pick(r, logKeyNouns) + " = self." + pick(r, logComponents) + "." + pick(r, logAdjectives) + "_" + pick(r, logKeyNouns) + "(" + pick(r, logKeyNouns) + `)\n`
		case styleNode:
			out[i] = "    at " + typ + "." + fn + " (/app/dist/" + pkg + "/" + pick(r, logComponents) + "/" + strings.ToLower(typ) + ".js:" + line + ":" + strconv.Itoa(1+r.IntN(80)) + `)\n`
		default:
			out[i] = "github.com/acme/" + pkg + "/internal/" + pick(r, logComponents) + ".(*" + typ + ")." + title(fn) + `(...)\n\t/build/internal/` + pick(r, logComponents) + "/" + strings.ToLower(typ) + ".go:" + line + " +0x" + strconv.FormatInt(int64(r.IntN(4096)), 16) + `\n`
		}
	}

	return out
}

// write writes the stream to w and returns its size.
func (g *logGen) write(w io.Writer) (int64, error) {
	bw := bufio.NewWriterSize(w, 1<<20)
	var line []byte
	var size int64
	for range g.opts.Events {
		line = g.next(line[:0])
		size += int64(len(line))
		if _, err := bw.Write(line); err != nil {
			return size, err
		}
	}

	return size, bw.Flush()
}

// next appends the next line of the stream to dst.
func (g *logGen) next(dst []byte) []byte {
	r := g.r
	span := float64(g.opts.Span.Microseconds())
	g.clock += r.ExpFloat64() * g.step / (1 + 0.35*math.Sin(g.clock/span*6*math.Pi))
	at := g.clock
	if g.burst.left > 0 {
		g.burst.left--
		g.burst.at += r.ExpFloat64() * 20_000

		return g.event(dst, g.burst.at, g.burst.svc, g.burst.pod)
	}
	switch u := r.Float64(); {
	case u < 0.0005:
		at -= (5 + 40*r.Float64()) * 60e6
	case u < 0.008:
		at -= r.ExpFloat64() * 90e6
	default:
		at -= math.Abs(r.NormFloat64()) * 150e3
	}
	if u := r.Float64(); u < g.opts.Text {
		ts := logsEpoch.Add(time.Duration(at) * time.Microsecond)
		line := dst
		if g.opts.WrapText {
			line = g.text[:0]
		}
		if u < g.opts.Text*0.7 {
			line = g.access(line, ts)
		} else {
			line = g.syslogLine(line, ts)
		}
		if !g.opts.WrapText {
			return line
		}
		g.text = line
		dst = append(dst, `{"ts":`...)
		dst = strconv.AppendInt(dst, logsEpoch.UnixMicro()+int64(at), 10)
		dst = append(dst, `,"_raw":"`...)
		for _, c := range line[:len(line)-1] {
			if c == '"' || c == '\\' {
				dst = append(dst, '\\')
			}
			dst = append(dst, c)
		}

		return append(dst, "\"}\n"...)
	}
	svc := &g.services[g.svcZipf.Uint64()]
	pods := svc.pods[0]
	if g.clock/span >= svc.deployAt {
		pods = svc.pods[1]
	}
	pod := &pods[r.IntN(len(pods))]
	if r.IntN(250_000) == 0 {
		g.burst.left, g.burst.svc, g.burst.pod = 500+r.IntN(2500), svc, pod
		g.burst.at = g.clock - (10+40*r.Float64())*60e6
	}

	return g.event(dst, at, svc, pod)
}

func (g *logGen) status(level int) int {
	r := g.r
	switch {
	case level >= logError:
		return [...]int{500, 500, 500, 500, 500, 500, 502, 503, 503, 504}[r.IntN(10)]
	case level == logWarn:
		return [...]int{400, 401, 403, 404, 404, 409, 422, 429, 429, 429}[r.IntN(10)]
	}

	return [...]int{200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 201, 204, 304, 404}[r.IntN(20)]
}

// concrete appends a route with its identifiers filled in.
func (g *logGen) concrete(dst []byte, route string, uuids bool) []byte {
	r := g.r
	for {
		open := strings.Index(route, "{id}")
		if open < 0 {
			break
		}
		dst = append(dst, route[:open]...)
		switch {
		case uuids:
			dst = g.uuid(dst)
		case strings.HasSuffix(route[:open], "/users/"):
			dst = strconv.AppendUint(dst, g.userZipf.Uint64()+1, 10)
		default:
			dst = strconv.AppendInt(dst, int64(1+r.IntN(5_000_000)), 10)
		}
		route = route[open+4:]
	}
	dst = append(dst, route...)
	switch r.IntN(12) {
	case 0, 1:
		dst = append(dst, "?page="...)
		dst = strconv.AppendInt(dst, int64(1+r.IntN(40)), 10)
		dst = append(dst, "&limit=50"...)
	case 2:
		dst = append(dst, "?q="...)
		dst = append(dst, pick(r, logKeyNouns)...)
	case 3:
		dst = append(dst, "?cursor="...)
		dst = g.hex(dst, 24)
	}

	return dst
}

func (g *logGen) number(dst []byte, v float64, whole bool) []byte {
	if whole {
		return strconv.AppendInt(dst, int64(math.Round(v)), 10)
	}

	return strconv.AppendFloat(dst, math.Round(v*100)/100, 'f', -1, 64)
}

func member(dst []byte, key, value string) []byte {
	dst = append(dst, '"')
	dst = append(dst, key...)
	dst = append(dst, `":"`...)
	dst = append(dst, value...)

	return append(dst, `",`...)
}

// event appends one JSON event of a pod at the time at, in microseconds
// since logsEpoch.
func (g *logGen) event(dst []byte, at float64, svc *logService, pod *logPod) []byte {
	r := g.r
	progress := g.clock / float64(g.opts.Span.Microseconds())
	level := logFatal
	u := r.Float64()
	for l, share := range svc.levels {
		if u < share {
			level = l

			break
		}
	}
	if progress >= svc.incident[0] && progress < svc.incident[1] && r.IntN(100) < 35 {
		level = logError
	}
	request := false
	switch level {
	case logInfo:
		request = r.IntN(100) < 60
	case logWarn:
		request = r.IntN(100) < 35
	case logError, logFatal:
		request = r.IntN(100) < 45
	}
	failed := level >= logError && r.IntN(100) < 80
	var method, route string
	var status int
	var latency float64
	if request {
		method, route, status = pick(r, logMethods), g.routes[g.skewed(svc.routes)], g.status(level)
		g.path = g.concrete(g.path[:0], route, svc.uuids)
		latency = svc.latency * math.Exp(r.NormFloat64()*0.9)
		switch {
		case status == 504:
			latency = 30000 + 50*r.Float64()
		case r.IntN(64) == 0:
			latency *= 5 + 20*r.ExpFloat64()
		}
	}
	version := svc.versions[0]
	if progress >= svc.deployAt {
		version = svc.versions[1]
	}

	dst = append(dst, '{')
	for _, f := range logFieldOrder[svc.style] {
		switch f {
		case fTS:
			dst = append(dst, `"ts":`...)
			dst = strconv.AppendInt(dst, logsEpoch.UnixMicro()+int64(at), 10)
			dst = append(dst, ',')
		case fLevel:
			names := &logLevelNames[0]
			if svc.style == styleJava {
				names = &logLevelNames[1]
			}
			dst = member(dst, "level", names[level])
		case fService:
			dst = member(dst, "service", svc.name)
		case fEnv:
			dst = member(dst, "env", pod.env)
		case fHost:
			dst = member(dst, "host", pod.host)
		case fPod:
			dst = member(dst, "pod", pod.name)
		case fVersion:
			dst = member(dst, "version", version)
		case fLogger:
			dst = member(dst, "logger", pick(r, svc.loggers))
		case fThread:
			dst = append(dst, `"thread":"`...)
			dst = append(dst, [...]string{"http-nio-8080-exec-", "kafka-consumer-", "scheduling-", "grpc-default-executor-"}[r.IntN(4)]...)
			dst = strconv.AppendInt(dst, int64(1+r.IntN(48)), 10)
			dst = append(dst, `",`...)
		case fCaller:
			dst = member(dst, "caller", pick(r, svc.callers))
		case fLine:
			dst = append(dst, `"lineno":`...)
			dst = strconv.AppendInt(dst, int64(10+r.IntN(900)), 10)
			dst = append(dst, ',')
		case fPID:
			dst = append(dst, `"pid":`...)
			dst = strconv.AppendInt(dst, int64(1+r.IntN(30)), 10)
			dst = append(dst, ',')
		case fTrace:
			dst = append(dst, `"trace_id":"`...)
			dst = g.hex(dst, 32)
			dst = append(dst, `",`...)
		case fSpan:
			dst = append(dst, `"span_id":"`...)
			dst = g.hex(dst, 16)
			dst = append(dst, `",`...)
		case fUser:
			switch u := r.IntN(100); {
			case u < 70:
				dst = append(dst, `"user_id":`...)
				dst = strconv.AppendUint(dst, g.userZipf.Uint64()+1, 10)
				dst = append(dst, ',')
			case u < 76:
				dst = append(dst, `"user_id":null,`...)
			}
		case fMessage:
			dst = append(dst, `"message":"`...)
			switch {
			case !request:
				dst = g.render(dst, &g.templates[g.skewed(svc.templates[min(level, logError)])], svc)
			case svc.style == styleJava || svc.style == stylePython:
				dst = append(dst, method...)
				dst = append(dst, ' ')
				dst = append(dst, g.path...)
				dst = append(dst, ' ')
				dst = strconv.AppendInt(dst, int64(status), 10)
				dst = append(dst, " in "...)
				dst = g.number(dst, latency, true)
				dst = append(dst, "ms"...)
			default:
				dst = append(dst, "request completed"...)
			}
			dst = append(dst, `",`...)
		case fHTTP:
			if !request {
				continue
			}
			dst = append(dst, `"http":{`...)
			dst = member(dst, "method", method)
			dst = member(dst, "route", route)
			dst = member(dst, "path", string(g.path))
			dst = append(dst, `"status":`...)
			dst = strconv.AppendInt(dst, int64(status), 10)
			dst = append(dst, `,"latency_ms":`...)
			dst = g.number(dst, latency, svc.style == styleJava)
			dst = append(dst, `,"bytes":`...)
			dst = strconv.AppendInt(dst, int64(math.Exp(r.NormFloat64()*1.4+7)), 10)
			dst = append(dst, `,"client_ip":"`...)
			dst = g.ip(dst)
			dst = append(dst, '"')
			if r.IntN(5) < 2 {
				dst = append(dst, `,"user_agent":"`...)
				dst = append(dst, pick(r, logAgents)...)
				dst = append(dst, '"')
			}
			switch r.IntN(8) {
			case 0:
				dst = append(dst, `,"referer":null`...)
			case 1:
				dst = append(dst, `,"referer":"https://www.example.com`...)
				dst = append(dst, g.routes[r.IntN(len(g.routes))]...)
				dst = append(dst, '"')
			}
			dst = append(dst, "},"...)
		case fDuration:
			if request || r.IntN(10) >= 3 {
				continue
			}
			dst = append(dst, `"duration_ms":`...)
			dst = g.number(dst, 4*math.Exp(r.NormFloat64()*1.6), r.IntN(6) == 0)
			dst = append(dst, ',')
		case fError:
			switch {
			case failed:
				dst = g.failure(dst, svc)
			case svc.style == styleNode:
				dst = append(dst, `"error":null,`...)
			}
		case fCode:
			if svc.codeKind == 0 || (level < logWarn && r.IntN(10) > 0) {
				continue
			}
			code := int64(1000 + int(g.pickZipf.Uint64())*7 + r.IntN(3))
			if svc.codeKind == 1 {
				dst = append(dst, `"code":"E`...)
				dst = strconv.AppendInt(dst, code, 10)
				dst = append(dst, `",`...)
			} else {
				dst = append(dst, `"code":`...)
				dst = strconv.AppendInt(dst, code, 10)
				dst = append(dst, ',')
			}
		case fTags:
			if r.IntN(5) > 0 {
				continue
			}
			dst = append(dst, `"tags":[`...)
			for range 1 + r.IntN(4) {
				dst = append(dst, '"')
				dst = append(dst, pick(r, logTags)...)
				dst = append(dst, `",`...)
			}
			dst[len(dst)-1] = ']'
			dst = append(dst, ',')
		case fExtra:
			dst = g.extra(dst, svc)
		}
	}
	dst[len(dst)-1] = '}'

	return append(dst, '\n')
}

// failure appends an error object with a stack trace of the service's
// logger style.
func (g *logGen) failure(dst []byte, svc *logService) []byte {
	r := g.r
	typ, msg := pick(r, logErrorTypes[svc.style]), pick(r, logErrorMessages)
	dst = append(dst, `"error":{`...)
	dst = member(dst, "type", typ)
	dst = member(dst, "message", msg)
	dst = append(dst, `"stack":"`...)
	own := func(n int) {
		from := r.IntN(len(svc.frames))
		for i := range n {
			dst = append(dst, svc.frames[(from+i)%len(svc.frames)]...)
		}
	}
	shared := g.shared[svc.style]
	// A few entry points, so that the frames under a service's own repeat.
	under := func(n int) {
		from := r.IntN(4) * len(shared) / 4
		for i := range n {
			dst = append(dst, shared[(from+i)%len(shared)]...)
		}
	}
	switch svc.style {
	case styleJava:
		dst = append(dst, typ...)
		dst = append(dst, ": "...)
		dst = append(dst, msg...)
		dst = append(dst, `\n`...)
		own(4 + r.IntN(14))
		under(12 + r.IntN(30))
		if r.IntN(4) == 0 {
			dst = append(dst, `Caused by: `...)
			dst = append(dst, pick(r, logErrorTypes[styleJava])...)
			dst = append(dst, `: `...)
			dst = append(dst, pick(r, logErrorMessages)...)
			dst = append(dst, `\n`...)
			own(3 + r.IntN(8))
			dst = append(dst, `\t... `...)
			dst = strconv.AppendInt(dst, int64(10+r.IntN(40)), 10)
			dst = append(dst, ` more\n`...)
		}
	case stylePython:
		dst = append(dst, `Traceback (most recent call last):\n`...)
		under(2 + r.IntN(4))
		own(2 + r.IntN(8))
		dst = append(dst, typ...)
		dst = append(dst, ": "...)
		dst = append(dst, msg...)
	case styleNode:
		dst = append(dst, typ...)
		dst = append(dst, ": "...)
		dst = append(dst, msg...)
		dst = append(dst, `\n`...)
		own(2 + r.IntN(8))
		under(4 + r.IntN(8))
	default:
		dst = append(dst, "panic: "...)
		dst = append(dst, msg...)
		dst = append(dst, `\n\ngoroutine `...)
		dst = strconv.AppendInt(dst, int64(1+r.IntN(90000)), 10)
		dst = append(dst, ` [running]:\n`...)
		own(3 + r.IntN(9))
		under(3 + r.IntN(5))
	}

	return append(dst, `"},`...)
}

// extra appends a few of the service's rare members.
func (g *logGen) extra(dst []byte, svc *logService) []byte {
	r := g.r
	g.drawn = g.drawn[:0]
	draw := func(i int) {
		at := len(g.drawn)
		for j, have := range g.drawn {
			if have == i {
				return
			}
			if g.keys[have].order > g.keys[i].order && at == len(g.drawn) {
				at = j
			}
		}
		g.drawn = append(g.drawn, 0)
		copy(g.drawn[at+1:], g.drawn[at:])
		g.drawn[at] = i
	}
	if len(svc.keys) > 0 {
		for range [...]int{0, 0, 0, 1, 1, 1, 2, 2, 3, 4}[r.IntN(10)] {
			i := 0
			if svc.keyZipf != nil {
				i = int(svc.keyZipf.Uint64())
			}
			draw(svc.keys[i])
		}
	}
	if r.IntN(10) == 0 {
		draw(g.skewed(g.global))
	}
	open, ns, group := 0, "", ""
	closeTo := func(depth int) {
		for ; open > depth; open-- {
			dst[len(dst)-1] = '}'
			dst = append(dst, ',')
		}
	}
	for _, i := range g.drawn {
		k := &g.keys[i]
		switch {
		case k.ns != ns:
			closeTo(0)
		case k.group != group:
			closeTo(1)
		}
		if open == 0 {
			dst = append(dst, '"')
			dst = append(dst, k.ns...)
			dst = append(dst, `":{`...)
			ns, group, open = k.ns, "", 1
		}
		if k.group != "" && open == 1 {
			dst = append(dst, '"')
			dst = append(dst, k.group...)
			dst = append(dst, `":{`...)
			group, open = k.group, 2
		}
		dst = append(dst, '"')
		dst = append(dst, k.name...)
		dst = append(dst, `":`...)
		kind := k.kind
		if kind == keyMixed {
			kind = [...]int{keyInt, keyID}[r.IntN(2)]
		}
		switch kind {
		case keyBool:
			dst = strconv.AppendBool(dst, r.IntN(3) > 0)
		case keyLabel:
			dst = append(dst, '"')
			dst = append(dst, pick(r, k.labels)...)
			dst = append(dst, '"')
		case keyInt:
			dst = strconv.AppendInt(dst, int64(r.IntN(10000)), 10)
		case keyFloat:
			dst = g.number(dst, 50*r.ExpFloat64(), false)
		default:
			dst = append(dst, '"')
			dst = g.hex(dst, 12)
			dst = append(dst, '"')
		}
		dst = append(dst, ',')
	}
	closeTo(0)

	return dst
}

// render appends a message with its slots filled in.
func (g *logGen) render(dst []byte, t *logTemplate, svc *logService) []byte {
	r := g.r
	for i, slot := range t.slots {
		dst = append(dst, t.lits[i]...)
		switch slot {
		case slotN:
			dst = strconv.AppendInt(dst, int64(math.Exp(r.Float64()*8)), 10)
		case slotID:
			if r.IntN(5) < 3 {
				dst = strconv.AppendUint(dst, g.userZipf.Uint64()+1, 10)
			} else {
				dst = strconv.AppendInt(dst, int64(1_000_000+r.IntN(900_000_000)), 10)
			}
		case slotMS:
			dst = strconv.AppendFloat(dst, 3*math.Exp(r.NormFloat64()*1.8), 'f', 1+r.IntN(3), 64)
		case slotBytes:
			dst = strconv.AppendInt(dst, int64(math.Exp(r.NormFloat64()*1.5+8)), 10)
		case slotHex:
			dst = g.hex(dst, 16)
		case slotUUID:
			dst = g.uuid(dst)
		case slotKey:
			switch r.IntN(4) {
			case 0:
				dst = append(dst, "user:"...)
				dst = strconv.AppendUint(dst, g.userZipf.Uint64()+1, 10)
				dst = append(dst, ":profile"...)
			case 1:
				dst = append(dst, "sess:"...)
				dst = g.hex(dst, 8)
			case 2:
				dst = append(dst, "cart:"...)
				dst = strconv.AppendInt(dst, int64(r.IntN(5_000_000)), 10)
			default:
				dst = append(dst, "product:"...)
				dst = strconv.AppendInt(dst, int64(r.IntN(200_000)), 10)
				dst = append(dst, ":price"...)
			}
		case slotQuoted:
			dst = append(dst, `\"`...)
			dst = append(dst, pick(r, logQuoted)...)
			dst = append(dst, `\"`...)
		case slotPath:
			switch r.IntN(4) {
			case 0:
				dst = append(dst, "/var/lib/"...)
				dst = append(dst, svc.noun...)
				dst = append(dst, "/segments/"...)
				dst = g.hex(dst, 8)
				dst = append(dst, ".seg"...)
			case 1:
				dst = append(dst, "/app/config/"...)
				dst = append(dst, pick(r, logKeyNouns)...)
				dst = append(dst, ".yaml"...)
			case 2:
				dst = append(dst, "/data/"...)
				dst = append(dst, pick(r, logResources)...)
				dst = append(dst, "/2026/03/part-"...)
				dst = strconv.AppendInt(dst, int64(10000+r.IntN(80000)), 10)
				dst = append(dst, ".parquet"...)
			default:
				dst = append(dst, "/tmp/upload-"...)
				dst = g.hex(dst, 12)
				dst = append(dst, ".tmp"...)
			}
		case slotHost:
			switch r.IntN(4) {
			case 0:
				dst = append(dst, svc.noun...)
				dst = append(dst, "-db-"...)
				dst = strconv.AppendInt(dst, int64(r.IntN(6)), 10)
				dst = append(dst, ".internal"...)
			case 1:
				dst = append(dst, "redis-"...)
				dst = strconv.AppendInt(dst, int64(r.IntN(12)), 10)
				dst = append(dst, ".cache.svc.cluster.local"...)
			case 2:
				dst = append(dst, "kafka-"...)
				dst = strconv.AppendInt(dst, int64(r.IntN(9)), 10)
				dst = append(dst, ".broker.internal"...)
			default:
				other := &g.services[r.IntN(len(g.services))]
				dst = append(dst, pick(r, other.pods[0]).name...)
			}
		case slotPort:
			dst = strconv.AppendInt(dst, int64(pick(r, logPorts)), 10)
		case slotIP:
			dst = g.ip(dst)
		case slotEmail:
			dst = append(dst, pick(r, logAdjectives)...)
			dst = strconv.AppendInt(dst, int64(r.IntN(100000)), 10)
			dst = append(dst, "@example."...)
			dst = append(dst, [...]string{"com", "org", "net", "io"}[r.IntN(4)]...)
		case slotMoney:
			dst = append(dst, [...]string{"$", "EUR ", "GBP "}[r.IntN(3)]...)
			dst = strconv.AppendFloat(dst, 200*r.ExpFloat64(), 'f', 2, 64)
		case slotWord:
			dst = append(dst, pick(r, logKeyNouns)...)
		case slotBool:
			dst = strconv.AppendBool(dst, r.IntN(2) == 0)
		case slotRoute:
			dst = append(dst, g.routes[pick(r, svc.routes)]...)
		}
	}

	return append(dst, t.lits[len(t.slots)]...)
}

// access appends a line of an access log in the combined format.
func (g *logGen) access(dst []byte, ts time.Time) []byte {
	r := g.r
	dst = g.ip(dst)
	dst = append(dst, " - "...)
	if r.IntN(4) == 0 {
		dst = append(dst, pick(r, logAdjectives)...)
		dst = strconv.AppendInt(dst, int64(r.IntN(5000)), 10)
	} else {
		dst = append(dst, '-')
	}
	dst = ts.AppendFormat(append(dst, " ["...), "02/Jan/2006:15:04:05 -0700")
	dst = append(dst, `] "`...)
	dst = append(dst, pick(r, logMethods)...)
	dst = append(dst, ' ')
	dst = g.concrete(dst, g.routes[int(g.pickZipf.Uint64())*3%len(g.routes)], r.IntN(9) == 0)
	dst = append(dst, ` HTTP/1.1" `...)
	level := logInfo
	switch u := r.IntN(100); {
	case u < 2:
		level = logError
	case u < 9:
		level = logWarn
	}
	dst = strconv.AppendInt(dst, int64(g.status(level)), 10)
	dst = append(dst, ' ')
	dst = strconv.AppendInt(dst, int64(math.Exp(r.NormFloat64()*1.4+7)), 10)
	if r.IntN(3) == 0 {
		dst = append(dst, ` "https://www.example.com`...)
		dst = append(dst, g.routes[r.IntN(len(g.routes))]...)
		dst = append(dst, `" "`...)
	} else {
		dst = append(dst, ` "-" "`...)
	}
	dst = append(dst, pick(r, logAgents)...)

	return append(dst, "\"\n"...)
}

// syslogLine appends a line as a host's syslog daemon writes it.
func (g *logGen) syslogLine(dst []byte, ts time.Time) []byte {
	r := g.r
	dst = ts.AppendFormat(dst, time.Stamp)
	dst = append(dst, ' ')
	if r.IntN(5) == 0 {
		dst = append(dst, pick(r, g.edges)...)
	} else {
		node := pick(r, g.nodes)
		dst = append(dst, node[:strings.IndexByte(node, '.')]...)
	}
	dst = append(dst, ' ')
	dst = g.render(dst, &g.syslog[int(g.pickZipf.Uint64())%len(g.syslog)], &g.services[r.IntN(len(g.services))])

	return append(dst, '\n')
}

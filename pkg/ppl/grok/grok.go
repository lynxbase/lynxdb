// Package grok expands grok patterns into RE2 regular expressions with
// named capture groups.
//
// A reference %{NAME:field} captures the text NAME matches into field. A
// top-level reference without a field name, %{NAME}, captures into a
// column named NAME, and the named references inside NAME's definition
// capture as well; unnamed references nested in definitions do not
// capture. A :type suffix (%{NUMBER:n:int}) is accepted and ignored: grok
// captures are strings.
package grok

import (
	"fmt"
	"regexp"
)

// Patterns is the built-in pattern library, a subset of the standard grok
// patterns used by Logstash and OpenSearch.
var Patterns = map[string]string{
	"USERNAME":          `[a-zA-Z0-9._-]+`,
	"USER":              `%{USERNAME}`,
	"EMAILLOCALPART":    `[a-zA-Z0-9!#$%&'*+/=?^_{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_{|}~-]+)*`,
	"EMAILADDRESS":      `%{EMAILLOCALPART}@%{HOSTNAME}`,
	"INT":               `(?:[+-]?(?:[0-9]+))`,
	"BASE10NUM":         `(?:[+-]?(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+))`,
	"NUMBER":            `(?:%{BASE10NUM})`,
	"BASE16NUM":         `(?:0[xX]?[0-9a-fA-F]+)`,
	"BASE16FLOAT":       `\b[+-]?(?:0[xX])?(?:[0-9A-Fa-f]+(?:\.[0-9A-Fa-f]*)?|\.[0-9A-Fa-f]+)\b`,
	"POSINT":            `\b(?:[1-9][0-9]*)\b`,
	"NONNEGINT":         `\b(?:[0-9]+)\b`,
	"WORD":              `\b\w+\b`,
	"NOTSPACE":          `\S+`,
	"SPACE":             `\s*`,
	"DATA":              `.*?`,
	"GREEDYDATA":        `.*`,
	"QUOTEDSTRING":      `"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'`,
	"QS":                `%{QUOTEDSTRING}`,
	"UUID":              `[A-Fa-f0-9]{8}-(?:[A-Fa-f0-9]{4}-){3}[A-Fa-f0-9]{12}`,
	"MAC":               `(?:%{CISCOMAC}|%{WINDOWSMAC}|%{COMMONMAC})`,
	"CISCOMAC":          `(?:(?:[A-Fa-f0-9]{4}\.){2}[A-Fa-f0-9]{4})`,
	"WINDOWSMAC":        `(?:(?:[A-Fa-f0-9]{2}-){5}[A-Fa-f0-9]{2})`,
	"COMMONMAC":         `(?:(?:[A-Fa-f0-9]{2}:){5}[A-Fa-f0-9]{2})`,
	"IPV6":              `(?:(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}|(?:[0-9A-Fa-f]{1,4}:){1,7}:|(?:[0-9A-Fa-f]{1,4}:){1,6}:[0-9A-Fa-f]{1,4}|::(?:[0-9A-Fa-f]{1,4}:){0,5}[0-9A-Fa-f]{1,4}|::)`,
	"IPV4":              `(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)`,
	"IP":                `(?:%{IPV6}|%{IPV4})`,
	"HOSTNAME":          `\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*\.?\b`,
	"IPORHOST":          `(?:%{IP}|%{HOSTNAME})`,
	"HOSTPORT":          `%{IPORHOST}:%{POSINT}`,
	"PATH":              `(?:%{UNIXPATH}|%{WINPATH})`,
	"UNIXPATH":          `(?:/[\w_%!$@:.,+~-]*)+`,
	"WINPATH":           `(?:[A-Za-z]+:|\\)(?:\\[^\\?*]*)+`,
	"URIPROTO":          `[A-Za-z][A-Za-z0-9+\-.]+`,
	"URIHOST":           `%{IPORHOST}(?::%{POSINT})?`,
	"URIPATH":           `(?:/[A-Za-z0-9$.+!*'(){},~:;=@#%&_\-]*)+`,
	"URIPARAM":          `\?[A-Za-z0-9$.+!*'|(){},~@#%&/=:;_?\-\[\]<>]*`,
	"URIPATHPARAM":      `%{URIPATH}(?:%{URIPARAM})?`,
	"URI":               `%{URIPROTO}://(?:%{USER}(?::[^@]*)?@)?(?:%{URIHOST})?(?:%{URIPATHPARAM})?`,
	"MONTH":             `\b(?:[Jj]an(?:uary)?|[Ff]eb(?:ruary)?|[Mm]ar(?:ch)?|[Aa]pr(?:il)?|[Mm]ay|[Jj]un(?:e)?|[Jj]ul(?:y)?|[Aa]ug(?:ust)?|[Ss]ep(?:tember)?|[Oo]ct(?:ober)?|[Nn]ov(?:ember)?|[Dd]ec(?:ember)?)\b`,
	"MONTHNUM":          `(?:0?[1-9]|1[0-2])`,
	"MONTHDAY":          `(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])`,
	"DAY":               `(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)`,
	"YEAR":              `(?:\d\d){1,2}`,
	"HOUR":              `(?:2[0123]|[01]?[0-9])`,
	"MINUTE":            `(?:[0-5][0-9])`,
	"SECOND":            `(?:(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?)`,
	"TIME":              `%{HOUR}:%{MINUTE}(?::%{SECOND})?`,
	"DATE_US":           `%{MONTHNUM}[/-]%{MONTHDAY}[/-]%{YEAR}`,
	"DATE_EU":           `%{MONTHDAY}[./-]%{MONTHNUM}[./-]%{YEAR}`,
	"DATE":              `%{DATE_US}|%{DATE_EU}`,
	"ISO8601_TIMEZONE":  `(?:Z|[+-]%{HOUR}(?::?%{MINUTE}))`,
	"ISO8601_SECOND":    `%{SECOND}`,
	"TIMESTAMP_ISO8601": `%{YEAR}-%{MONTHNUM}-%{MONTHDAY}[T ]%{HOUR}:?%{MINUTE}(?::?%{SECOND})?%{ISO8601_TIMEZONE}?`,
	"DATESTAMP":         `%{DATE}[- ]%{TIME}`,
	"TZ":                `(?:[APMCE][SD]T|UTC)`,
	"HTTPDATE":          `%{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}`,
	"SYSLOGTIMESTAMP":   `%{MONTH} +%{MONTHDAY} %{TIME}`,
	"PROG":              `[\x21-\x5a\x5c\x5e-\x7e]+`,
	"SYSLOGPROG":        `%{PROG:program}(?:\[%{POSINT:pid}\])?`,
	"SYSLOGHOST":        `%{IPORHOST}`,
	"SYSLOGBASE":        `%{SYSLOGTIMESTAMP:timestamp} (?:%{SYSLOGFACILITY} )?%{SYSLOGHOST:logsource} %{SYSLOGPROG}:`,
	"SYSLOGFACILITY":    `<%{NONNEGINT:facility}.%{NONNEGINT:priority}>`,
	"LOGLEVEL":          `(?:[Aa]lert|ALERT|[Tt]race|TRACE|[Dd]ebug|DEBUG|[Nn]otice|NOTICE|[Ii]nfo|INFO|[Ww]arn?(?:ing)?|WARN?(?:ING)?|[Ee]rr?(?:or)?|ERR?(?:OR)?|[Cc]rit?(?:ical)?|CRIT?(?:ICAL)?|[Ff]atal|FATAL|[Ss]evere|SEVERE|EMERG(?:ENCY)?|[Ee]merg(?:ency)?)`,
	"COMMONAPACHELOG":   `%{IPORHOST:clientip} %{HTTPDUSER:ident} %{USER:auth} \[%{HTTPDATE:timestamp}\] "(?:%{WORD:verb} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})" %{NUMBER:response} (?:%{NUMBER:bytes}|-)`,
	"COMBINEDAPACHELOG": `%{COMMONAPACHELOG} %{QS:referrer} %{QS:agent}`,
	"HTTPDUSER":         `%{EMAILADDRESS}|%{USER}`,
}

var refRe = regexp.MustCompile(`%\{(\w+)(?::([\w@.\[\]-]+))?(?::\w+)?\}`)

var fieldName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Compiled is an expanded grok pattern.
type Compiled struct {
	// Regex is the RE2 expression; each captured field is a named group.
	Regex string
	// Fields lists the captured field names in pattern order.
	Fields []string
}

// Compile expands a grok pattern.
func Compile(pattern string) (*Compiled, error) {
	c := &Compiled{}
	seen := map[string]bool{}
	re, err := expand(pattern, 0, true, c, seen)
	if err != nil {
		return nil, err
	}
	if _, err := regexp.Compile(re); err != nil {
		return nil, fmt.Errorf("grok pattern expands to an invalid regular expression: %w", err)
	}
	c.Regex = re
	return c, nil
}

func expand(pattern string, depth int, top bool, c *Compiled, seen map[string]bool) (string, error) {
	if depth > 20 {
		return "", fmt.Errorf("grok pattern definitions nest too deeply")
	}
	var errOut error
	out := refRe.ReplaceAllStringFunc(pattern, func(m string) string {
		if errOut != nil {
			return ""
		}
		sub := refRe.FindStringSubmatch(m)
		name, field := sub[1], sub[2]
		def, ok := Patterns[name]
		if !ok {
			errOut = fmt.Errorf("unknown grok pattern %%{%s}", name)
			return ""
		}
		if field == "" && top {
			field = name
		}
		inner, err := expand(def, depth+1, false, c, seen)
		if err != nil {
			errOut = err
			return ""
		}
		if field == "" {
			return "(?:" + inner + ")"
		}
		if !fieldName.MatchString(field) {
			errOut = fmt.Errorf("grok field name %q must be a plain identifier", field)
			return ""
		}
		if seen[field] {
			return "(?:" + inner + ")"
		}
		seen[field] = true
		c.Fields = append(c.Fields, field)
		return "(?P<" + field + ">" + inner + ")"
	})
	if errOut != nil {
		return "", errOut
	}
	return out, nil
}

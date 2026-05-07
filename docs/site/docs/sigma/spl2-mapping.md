# Sigma to SPL2 mapping

[Back to Sigma docs](index.md)

> LynxDB search precedence is `NOT > OR > AND`. OR binds tighter than AND.
> rsigma inserts the parentheses needed for its generated output, but keep this
> in mind when reading or editing a query by hand.

| Sigma | rsigma SPL2 fragment | LynxDB feature |
|---|---|---|
| `field: value` | `field="value"` | Search predicate |
| `field: 42` | `field=42` | Search predicate |
| `field: true` / `field: false` | `field=true` / `field=false` | Boolean literal |
| `field: null` | `NOT field=*` | Wildcard existence |
| `field|exists: true` | `field=*` | Wildcard existence |
| `field|contains: x` | `field=*"x"*` | Search glob |
| `field|startswith: x` | `field="x"*` | Search glob |
| `field|endswith: x` | `field=*"x"` | Search glob |
| `field|cased: X` | `field=CASE("X")` | `CASE()` directive |
| `field|re: pat` | `* | where field =~ "pat"` | Regex match operator |
| `field|cidr: 10.0.0.0/8` | `* | where cidrmatch("10.0.0.0/8", field)` | `cidrmatch()` function |
| `field|gte: 10` | `field>=10` | Comparison operator |
| `field|gt: 10` | `field>10` | Comparison operator |
| `field|lte: 10` | <code>field&lt;=10</code> | Comparison operator |
| `field|lt: 10` | <code>field&lt;10</code> | Comparison operator |
| `field: [a, b, c]` | `field IN ("a", "b", "c")` | `IN` list |
| keyword `kw` | `"kw"` | Full-text search predicate |
| `selA and not selB` | Parenthesized A with `AND NOT` B | Search composition |
| logsource to custom index | `set_state index=security` to `FROM security | search` | `FROM security` source clause |

rsigma may emit `format=minimal` output without the `FROM main | search`
prefix. That form is useful when embedding the predicate inside a larger SPL2
pipeline, as shown in the [cookbook](cookbook.md).

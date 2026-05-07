# CloudTrail

[Back to Sigma docs](../index.md)

This flow uses CloudTrail JSON events stored in a LynxDB index named `aws`.

Create one CloudTrail event and ingest it:

```bash
lynxdb server
printf '%s\n' '{"eventSource":"iam.amazonaws.com","eventName":"CreateUser","userIdentity":{"arn":"arn:aws:iam::123456789012:user/admin"},"sourceIPAddress":"203.0.113.10"}' > cloudtrail.ndjson
lynxdb ingest cloudtrail.ndjson --source cloudtrail --sourcetype json --index aws
```

Create an rsigma pipeline for the `aws` index:

```bash
cat > cloudtrail-lynxdb.yml <<'YAML'
transformations:
  - type: set_state
    key: index
    value: aws
YAML
```

Convert the AWS rule:

```bash
cat > cloudtrail-create-user.yml <<'YAML'
title: CloudTrail IAM CreateUser
logsource:
  product: aws
  service: cloudtrail
detection:
  selection:
    eventSource: iam.amazonaws.com
    eventName: CreateUser
  condition: selection
YAML

rsigma convert -t lynxdb -p cloudtrail-lynxdb.yml cloudtrail-create-user.yml > aws.spl2
```

Run a converted rule:

```bash
lynxdb query "$(cat aws.spl2)" --since 24h --format table
```

If the rule pack expects ECS or OCSF field names, add field mappings in the
rsigma pipeline before conversion.

To convert a checked-out SigmaHQ AWS pack after validating the single-rule
flow:

```bash
git clone https://github.com/SigmaHQ/sigma.git sigma
rsigma convert -t lynxdb -p cloudtrail-lynxdb.yml sigma/rules/cloud/aws > aws-pack.spl2
```

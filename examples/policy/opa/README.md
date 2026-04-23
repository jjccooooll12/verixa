# Verixa OPA Example

This example shows how to consume `verixa check --format policy-v1` from OPA without embedding policy logic inside Verixa itself.

## Files
- `input.policy-v1.json`: example Verixa output document
- `verixa_policy.rego`: sample policy rules

## Example Flow

Generate the Verixa policy document:

```bash
verixa check --format policy-v1 > verixa-policy.json
```

Evaluate it with OPA:

```bash
opa eval \
  --format pretty \
  --data examples/policy/opa/verixa_policy.rego \
  --input verixa-policy.json \
  'data.verixa.deny'
```

## Sample Policy Behavior

The sample policy denies when:
- there is a `new` error finding
- there is a warning on a `high` criticality source
- the baseline is stale in `prod`

That keeps Verixa focused on producing stable, data-aware signals while OPA remains the decision layer.

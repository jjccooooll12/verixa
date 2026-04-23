package verixa

deny contains message if {
  finding := input.findings[_]
  finding.severity == "error"
  finding.lifecycle_status == "new"
  message := sprintf(
    "new error on %s: %s",
    [finding.source_name, finding.stable_code],
  )
}

deny contains message if {
  finding := input.findings[_]
  finding.severity == "warning"
  finding.source_criticality == "high"
  message := sprintf(
    "warning on high-criticality source %s: %s",
    [finding.source_name, finding.stable_code],
  )
}

deny contains message if {
  input.run.environment == "prod"
  finding := input.findings[_]
  finding.stable_code == "baseline.stale"
  message := "prod baseline is stale"
}

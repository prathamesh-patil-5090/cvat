### Fixed

- Failed chunk jobs more reliably preserve the original API error (e.g. HTTP 404)
  when failure metadata is briefly unavailable after RQ marks the job failed
  (<https://github.com/cvat-ai/cvat/issues/11147>)

from(bucket: "in")
  |> filter(fn: (r) => r._measurement == "nginx" and r._field == "bytes_sent") // not used, for now
  |> group(columns: ["uri"])
  |> window(every: 10s)
  |> count()
  |> to()
counter = 0

request = function()
  counter = counter + 1
  local key = "key" .. counter
  local body = "value-" .. counter
  return wrk.format(
    "PUT",
    "/v0/entity?id=" .. key,
    { ["Content-Type"] = "text/plain" },
    body
  )
end

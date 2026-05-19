counter = 0

request = function()
  counter = counter + 1
  return wrk.format("GET", "/v0/entity?id=key" .. counter)
end
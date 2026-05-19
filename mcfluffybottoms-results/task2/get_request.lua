wrk.method = "GET"
wrk.headers["Content-Type"] = "application/json"

local targets = {
    "http://127.0.0.1:8080/index.html",
    "http://127.0.0.1:8081/index.html"
}

request = function()
    local id = math.random(1, 1000000)
    local host = targets[math.random(#targets)]
    return wrk.format("GET", host .. "/v0/entity/" .. id, nil)
end
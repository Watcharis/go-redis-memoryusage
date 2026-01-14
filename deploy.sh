

env GOOS=linux GOARCH=amd64 go build -o go-redis-memusage main.go


kubectl cp ./go-redis-memusage nginx-565d788696-l2jaa:/root/redis-memusage/go-redis-memusage

echo "----------start building $1----------"
go build -race -buildmode=plugin ../mrapps/$1.go
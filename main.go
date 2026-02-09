package main

import (
    "market-denet/t2api"
)
func main() {
    bearer, number := t2api.Login()
    go t2api.WatchMarket(bearer, number)
}

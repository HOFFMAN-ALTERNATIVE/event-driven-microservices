// Command orders runs the orders service (HTTP API + saga tracker).
package main

import (
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/orders"
	"github.com/HOFFMAN-ALTERNATIVE/event-driven-microservices/internal/platform/service"
)

func main() { service.Main("orders", orders.Schema, orders.Run) }

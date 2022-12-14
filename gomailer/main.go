package main

import (
	"fmt"
	"kafkamailer/kafka"
)

func main() {
	fmt.Println("This is go mailer with kafka")
	kafka.ReadEvent()
}

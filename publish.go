package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func publis2h() error {

	mechanism, err := scram.Mechanism(scram.SHA256, "cHJpbWUtcXVldHphbC01MDk2JO3t0Y1mITw6SaKtGggNfCWKtg2MmYyBdflz9Bo", "7bd32a2813994c959f3feea989f01422")
	if err != nil {
		log.Fatalln(err)
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"prime-quetzal-5096-eu1-kafka.upstash.io:9092"},
		Topic:   "tbd.tugas.socmed",
		Dialer:  dialer,
	})

	defer w.Close()

	url := "http://128.199.176.197:7551/streaming"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Basic YTU3ZGUwODAtZjdiYy00MDIyLTkzZGMtNjEyZDJhZjU4ZDMxOg==")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	content := make([]byte, 0)
	count := 0

	for scanner.Scan() {
		inputLine := scanner.Text()
		inputLine = strings.ReplaceAll(inputLine, "\n", "")
		if "[{" == inputLine {
			content = append(content, '{')
		} else if "},{" == inputLine || "}" == inputLine || "}]" == inputLine {
			content = append(content, '}')
			var jsonMap map[string]interface{}
			if err := json.Unmarshal(content, &jsonMap); err != nil {
				continue
			}
			count++
			message := kafka.Message{
				Key:   []byte([]byte(strconv.Itoa(count))),
				Value: content,
			}

			go func() {
				err := w.WriteMessages(context.Background(), message)
				if err != nil {
					fmt.Printf("Error sending message: %v\n", err)
				}
			}()

			fmt.Println(string(content))
			// reset buffer
			content = make([]byte, 0)
			content = append(content, '{')
			fmt.Println("parsed: ", count)
		} else {
			content = append(content, []byte(inputLine)...)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

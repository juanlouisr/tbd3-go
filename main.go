package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type SocialMediaData struct {
	SocialMedia string `json:"social_media"`
	Timestamp   string `json:"timestamp"`
	Count       string `json:"count"`
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	dbname := os.Getenv("DB_NAME")

	fmt.Printf("Pass: %s\n", password)
	fmt.Printf("DB: %s\n", dbname)

	// Connect to the database
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a new router
	router := httprouter.New()

	router.GET("/republish", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		if err := publish(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		jsonData, err := json.Marshal(map[string]interface{}{
			"message": "success",
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)
	})

	// Define a handler function for the "/example" path with query parameters of 3 strings
	router.GET("/data", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		// Get the query parameters

		queryValues := r.URL.Query()

		param1 := queryValues.Get("social_media")
		param2 := queryValues.Get("start")
		param3 := queryValues.Get("end")

		fmt.Println(param2)
		fmt.Println(param3)

		// Execute a database query
		start, err := time.Parse("2006-01-02 15:04:05", param2)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		end, err := time.Parse("2006-01-02 15:04:05", param3)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Printf("start: %v\n", start.Format("2006-01-02 15:04:05"))
		fmt.Printf("end: %v\n", end.Format("2006-01-02 15:04:05"))

		// Execute a database query
		rows, err := db.Query(`SELECT social_media, timestamp, count FROM aggregator WHERE social_media = $1 AND timestamp between $2 AND $3`,
			param1, start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer rows.Close()

		// Iterate over the query results and write them to the response
		count := 0
		var data []SocialMediaData
		for rows.Next() {
			count++
			fmt.Println("count ", count)
			var social_media, timestamp, count string
			err := rows.Scan(&social_media, &timestamp, &count)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			temp := SocialMediaData{
				SocialMedia: social_media,
				Timestamp:   timestamp,
				Count:       count,
			}
			data = append(data, temp)
			fmt.Printf("data: %+v\n", temp)
			// fmt.Fprintf(w, "social_media: %s, timestamp: %s, count: %s\n", col1, col2, col3)
		}

		if err := rows.Err(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		jsonData, err := json.Marshal(map[string]interface{}{
			"data": data,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)

	})

	// Start the HTTP server
	log.Fatal(http.ListenAndServe(":9191", router))
}

func publish() error {

	user := os.Getenv("KAFKA_USER")
	pass := os.Getenv("KAFKA_PASS")
	broker := os.Getenv("KAFKA_BROKER")
	topic := os.Getenv("KAFKA_TOPIC")

	url := os.Getenv("STREAM_URL")
	token := os.Getenv("STREAM_TOKEN")

	mechanism, err := scram.Mechanism(scram.SHA256, user, pass)
	if err != nil {
		log.Fatalln(err)
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
		Dialer:  dialer,
	})

	defer w.Close()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", fmt.Sprintf("Basic %s", token))
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

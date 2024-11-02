package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/streadway/amqp"
)

var rabbitMQURL = "amqp://guest:guest@localhost:5672/"
var postgresWithRabbit = "postgres://postgres:postgres@localhost:5433/postgres"
var postgresWithoutRabbit = "postgres://postgres:postgres@localhost:5434/postgres"
var performanceFile string

// Definimos la conexión y el canal como variables globales
var rabbitConn *amqp.Connection
var rabbitChannel *amqp.Channel

func main() {
	mode := flag.String("mode", "rabbitmq", "Modo de procesamiento: 'rabbitmq' o 'directo'")
	flag.Parse()

	if *mode == "rabbitmq" {
		performanceFile = "performance_data_rabbitmq.csv"
		initRabbitMQConnection()        // Conexión y canal persistente a RabbitMQ
		defer closeRabbitMQConnection() // Cerramos la conexión al finalizar
	} else if *mode == "directo" {
		performanceFile = "performance_data_directo.csv"
	} else {
		log.Fatalf("Modo no válido: %s. Usa 'rabbitmq' o 'directo'.", *mode)
	}

	createCSV() // Crear el archivo CSV correspondiente

	switch *mode {
	case "rabbitmq":
		fmt.Println("Ejecutando consultas de forma asincrónica con RabbitMQ...")
		for i := 0; i < 3; i++ { // Múltiples consumidores en paralelo
			go consumeFromRabbitAndRunHeavyQuery(i)
		}
		for i := 0; i < 1000; i++ { // Enviamos 1000 solicitudes de consulta a la cola
			start := time.Now()
			publishToRabbit(fmt.Sprintf("Consulta %d", i), "query", start)
			time.Sleep(10 * time.Millisecond)
		}
	case "directo":
		fmt.Println("Ejecutando consultas de forma secuencial...")
		runDirectQueries()
	}

	fmt.Println("Finalizado el envío de solicitudes.")
	select {}
}

// Inicializa una conexión persistente y un canal a RabbitMQ
func initRabbitMQConnection() {
	var err error
	rabbitConn, err = amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("No se pudo conectar a RabbitMQ: %v", err)
	}

	rabbitChannel, err = rabbitConn.Channel()
	if err != nil {
		log.Fatalf("No se pudo abrir el canal: %v", err)
	}

	_, err = rabbitChannel.QueueDeclare("query_queue", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("No se pudo declarar la cola query_queue: %v", err)
	}
}

// Cierra la conexión y el canal de RabbitMQ
func closeRabbitMQConnection() {
	if rabbitChannel != nil {
		rabbitChannel.Close()
	}
	if rabbitConn != nil {
		rabbitConn.Close()
	}
}

// Crea el archivo CSV
func createCSV() {
	file, err := os.Create(performanceFile)
	if err != nil {
		log.Fatalf("Error creando archivo CSV: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"Metodo", "Consulta", "Tiempo (ms)", "Resultado", "Datos", "Timestamp"})
}

// Registra el tiempo de rendimiento en el CSV
func recordPerformance(method string, task string, duration time.Duration, success bool, data string) {
	file, err := os.OpenFile(performanceFile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Fatalf("Error abriendo archivo CSV: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	result := "Exitoso"
	if !success {
		result = "Fallido"
	}

	timestamp := time.Now().Format(time.RFC3339)
	writer.Write([]string{method, task, fmt.Sprintf("%.2f", float64(duration.Milliseconds())), result, data, timestamp})
}

// Publica un mensaje en RabbitMQ
func publishToRabbit(message string, taskType string, start time.Time) {
	err := rabbitChannel.Publish(
		"",              // exchange
		taskType+"_queue", // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
	if err != nil {
		log.Fatalf("Error al publicar en RabbitMQ: %v", err)
	}

	fmt.Printf("Publicado en RabbitMQ: %s (Tarea: %s)\n", message, taskType)
}

// Ejecuta consultas de ejemplo secuenciales
func runDirectQueries() {
	connPg, err := pgx.Connect(context.Background(), postgresWithoutRabbit)
	if err != nil {
		log.Fatalf("No se pudo conectar a PostgreSQL: %v", err)
	}
	defer connPg.Close(context.Background())

	queries := []string{
		"SELECT COUNT(*) FROM customer;",
		"SELECT COUNT(*) FROM rental;",
		"SELECT AVG(amount) FROM payment;",
	}

	for _, query := range queries {
		start := time.Now()
		var result interface{}
		err := connPg.QueryRow(context.Background(), query).Scan(&result)
		duration := time.Since(start)

		if err != nil {
			log.Printf("Error al ejecutar consulta: %v", err)
			recordPerformance("Directo", query, duration, false, "")
		} else {
			recordPerformance("Directo", query, duration, true, fmt.Sprintf("%v", result))
			fmt.Printf("Consulta directa '%s' ejecutada en: %v ms, Resultado: %v\n", query, duration.Milliseconds(), result)
		}
	}
}

// Consumidor que ejecuta la consulta de manera asincrónica
func consumeFromRabbitAndRunHeavyQuery(consumerID int) {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("No se pudo conectar a RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("No se pudo abrir el canal: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare("query_queue", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("No se pudo declarar la cola query_queue: %v", err)
	}

	msgs, err := ch.Consume("query_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error al consumir mensajes de consulta: %v", err)
	}

	connPg, err := pgx.Connect(context.Background(), postgresWithRabbit)
	if err != nil {
		log.Fatalf("No se pudo conectar a PostgreSQL: %v", err)
	}
	defer connPg.Close(context.Background())

	for msg := range msgs {
		start := time.Now()
		var result int
		query := "SELECT COUNT(*) FROM customer;"
		err := connPg.QueryRow(context.Background(), query).Scan(&result)
		duration := time.Since(start)

		if err != nil {
			log.Printf("Error al ejecutar consulta pesada: %v", err)
			recordPerformance("RabbitMQ", query, duration, false, "")
		} else {
			recordPerformance("RabbitMQ", query, duration, true, fmt.Sprintf("%v", result))
			fmt.Printf("Consulta ejecutada con RabbitMQ por consumidor %d en: %v ms, Resultado: %v\n", consumerID, duration.Milliseconds(), result)
		}
		fmt.Printf("Mensaje procesado: %s\n", string(msg.Body))
	}
}

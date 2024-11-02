package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/streadway/amqp"
)

var rabbitMQURL = "amqp://guest:guest@localhost:5672/"
var postgresWithRabbit = "postgres://postgres:postgres@localhost:5433/postgres"
var postgresWithoutRabbit = "postgres://postgres:postgres@localhost:5434/postgres"
var performanceFile string
var totalSuccess, totalFailures int

// Definimos la conexión y el canal como variables globales
var rabbitConn *amqp.Connection
var rabbitChannel *amqp.Channel

func main() {
	mode := flag.String("mode", "rabbitmq", "Modo de inserción: 'rabbitmq' o 'directo'")
	flag.Parse()

	if *mode == "rabbitmq" {
		performanceFile = "performance_data_rabbitmq.csv"
		// Establece la conexión y el canal persistente al inicio
		initRabbitMQConnection()
		defer closeRabbitMQConnection() // Cerramos la conexión al finalizar
	} else if *mode == "directo" {
		performanceFile = "performance_data_directo.csv"
	} else {
		log.Fatalf("Modo no válido: %s. Usa 'rabbitmq' o 'directo'.", *mode)
	}

	createCSV() // Crear el archivo CSV correspondiente

	switch *mode {
	case "rabbitmq":
		fmt.Println("Ejecutando inserciones y tareas adicionales con RabbitMQ...")
		go consumeFromRabbitAndInsert()
		go consumeFromRabbitAndSendEmail()
		go consumeFromRabbitAndCreateLog()
		for i := 0; i < 1000; i++ {
			process := time.Now()
			publishToRabbit(fmt.Sprintf("Mensaje %d", i), "insert")
			publishToRabbit(fmt.Sprintf("Email para usuario %d", i), "email")
			publishToRabbit(fmt.Sprintf("Log entry %d", i), "log")
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			processDuration := time.Since(process)
			recordPerformance("RabbitMQ", "Process", processDuration, true)
			time.Sleep(50 * time.Millisecond)
		}
	case "directo":
		fmt.Println("Ejecutando inserciones directas en PostgreSQL con simulación de tareas adicionales...")
		insertDirectToPostgres()
	}

	fmt.Println("Finalizado el envío de mensajes.")
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

    // Declarar colas al inicio
    _, err = rabbitChannel.QueueDeclare("insert_queue", true, false, false, false, nil)
    if err != nil {
        log.Fatalf("No se pudo declarar la cola insert_queue: %v", err)
    }

    _, err = rabbitChannel.QueueDeclare("email_queue", true, false, false, false, nil)
    if err != nil {
        log.Fatalf("No se pudo declarar la cola email_queue: %v", err)
    }

    _, err = rabbitChannel.QueueDeclare("log_queue", true, false, false, false, nil)
    if err != nil {
        log.Fatalf("No se pudo declarar la cola log_queue: %v", err)
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

	writer.Write([]string{"Metodo", "Tarea", "Tiempo (ms)", "Resultado"})
}

// Registra el tiempo de rendimiento en el CSV y éxito/fallo
func recordPerformance(method string, task string, duration time.Duration, success bool) {
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
		totalFailures++
	} else {
		totalSuccess++
	}

	writer.Write([]string{method, task, fmt.Sprintf("%.2f", float64(duration.Milliseconds())), result})
}

// Publica un mensaje en RabbitMQ usando la conexión y el canal persistentes
func publishToRabbit(message string, taskType string) {
    err := rabbitChannel.Publish(
        "",     // exchange
        taskType+"_queue", // routing key
        false,  // mandatory
        false,  // immediate
        amqp.Publishing{
            ContentType: "text/plain",
            Body:        []byte(message),
			// DeliveryMode: amqp.Transient, // No persistente
		},
    )
    if err != nil {
        log.Fatalf("Error al publicar en RabbitMQ: %v", err)
    }

    fmt.Printf("Publicado en RabbitMQ: %s (Tarea: %s)\n", message, taskType)
}

func consumeFromRabbitAndInsert() {
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

	// Declara la cola para asegurarse de que existe
	_, err = ch.QueueDeclare("insert_queue", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("No se pudo declarar la cola insert_queue: %v", err)
	}

	msgs, err := ch.Consume("insert_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error al consumir mensajes: %v", err)
	}

	connPg, err := pgx.Connect(context.Background(), postgresWithRabbit)
	if err != nil {
		log.Fatalf("No se pudo conectar a PostgreSQL: %v", err)
	}
	defer connPg.Close(context.Background())

	for msg := range msgs {
		start := time.Now()
		_, err := connPg.Exec(context.Background(), "INSERT INTO messages (content) VALUES ($1)", string(msg.Body))
		duration := time.Since(start)
		if err != nil {
			log.Printf("Error al insertar en PostgreSQL: %v", err)
			recordPerformance("RabbitMQ", "Insert", duration, false)
		} else {
			recordPerformance("RabbitMQ", "Insert", duration, true)
			fmt.Printf("Insertado en PostgreSQL con RabbitMQ: %s (Tiempo: %v ms)\n", msg.Body, duration.Milliseconds())
		}
	}
}

func consumeFromRabbitAndSendEmail() {
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

	// Declara la cola para asegurarse de que existe
	_, err = ch.QueueDeclare("email_queue", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("No se pudo declarar la cola email_queue: %v", err)
	}

	msgs, err := ch.Consume("email_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error al consumir mensajes de email: %v", err)
	}

	for msg := range msgs {
		start := time.Now()
		time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond) // Simulación de envío de email
		duration := time.Since(start)
		recordPerformance("RabbitMQ", "Email", duration, true)
		fmt.Printf("Email enviado: %s (Tiempo: %v ms)\n", msg.Body, duration.Milliseconds())
	}
}

func consumeFromRabbitAndCreateLog() {
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

	// Declara la cola para asegurarse de que existe
	_, err = ch.QueueDeclare("log_queue", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("No se pudo declarar la cola log_queue: %v", err)
	}

	msgs, err := ch.Consume("log_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Error al consumir mensajes de log: %v", err)
	}

	for msg := range msgs {
		start := time.Now()
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond) // Simulación de creación de log
		duration := time.Since(start)
		recordPerformance("RabbitMQ", "Log", duration, true)
		fmt.Printf("Log creado: %s (Tiempo: %v ms)\n", msg.Body, duration.Milliseconds())
	}
}

// Función para realizar inserciones directas y simular tareas sin RabbitMQ
func insertDirectToPostgres() {
	start := time.Now()
	connPg, err := pgx.Connect(context.Background(), postgresWithoutRabbit)
	if err != nil {
		log.Fatalf("No se pudo conectar a PostgreSQL: %v", err)
	}
	defer connPg.Close(context.Background())

	for i := 0; i < 1000; i++ { 
		// Inserción directa en PostgreSQL
		process := time.Now()
		message := fmt.Sprintf("Mensaje directo %d", i)
		start := time.Now()
		_, err := connPg.Exec(context.Background(), "INSERT INTO messages (content) VALUES ($1)", message)
		duration := time.Since(start)
		if err != nil {
			log.Printf("Error al insertar directamente en PostgreSQL: %v", err)
			recordPerformance("Directo", "Insert", duration, false)
		} else {
			recordPerformance("Directo", "Insert", duration, true)
			fmt.Printf("Insertado directamente en PostgreSQL: %s (Tiempo: %v ms)\n", message, duration.Milliseconds())
		}

		// Simulación de envío de email
		start = time.Now()
		time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond) // simulacion tiempo random
		duration = time.Since(start)
		recordPerformance("Directo", "Email", duration, true)
		fmt.Printf("Email enviado: %s (Tiempo: %v ms)\n", message)

		// Simulación de creación de log
		start = time.Now()
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond) // simulacion tiempo random
		duration = time.Since(start)
		recordPerformance("Directo", "Log", duration, true)
		fmt.Printf("Log creado: %s (Tiempo: %v ms)\n", message)
		processDuration := time.Since(process);
		recordPerformance("Directo", "Process", processDuration, true)
		time.Sleep(50 * time.Millisecond) // simulador de peticiones
	}

	duration := time.Since(start)
	fmt.Printf("Tiempo total de procesamiento en modo Directo: %v ms\n", duration.Milliseconds())
}

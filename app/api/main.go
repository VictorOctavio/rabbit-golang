package main

import (
	"encoding/csv"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

type Record struct {
	Method   string  `json:"method"`
	Task     string  `json:"task"`
	Duration float64 `json:"duration"`
}

func main() {
	r := gin.Default()
	
	// Middleware de CORS
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		c.Next()
	})

	r.GET("/performance", getPerformanceData)
	r.GET("/system", getSystemUsage)

	r.Run(":8080")
}

// Lee ambos archivos CSV y devuelve datos de rendimiento para todas las tareas
func getPerformanceData(c *gin.Context) {
	// Cargar datos de ambos archivos CSV (RabbitMQ y Directo)
	rabbitData, err := readCSV("performance_data_rabbitmq.csv")
	if err != nil {
		log.Printf("Error al leer archivo RabbitMQ: %v", err)
	}
	directData, err := readCSV("performance_data_directo.csv")
	if err != nil {
		log.Printf("Error al leer archivo Directo: %v", err)
	}

	// Combina los datos
	records := append(rabbitData, directData...)

	// Calcula el promedio de cada tarea y método
	rabbitAvg := calculateAverages(rabbitData)
	directAvg := calculateAverages(directData)

	// Envía la respuesta JSON
	c.JSON(http.StatusOK, gin.H{
		"records":     records,
		"rabbitAvg":   rabbitAvg,
		"directAvg":   directAvg,
	})
}

// Función auxiliar para leer un archivo CSV y asignarle las tareas correspondientes
func readCSV(filename string) ([]Record, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, _ = reader.Read() // Omitir la cabecera

	var records []Record
	for {
		line, err := reader.Read()
		if err != nil {
			break
		}
		duration, _ := strconv.ParseFloat(line[2], 64) // La duración está en la tercera columna
		records = append(records, Record{Method: line[0], Task: line[1], Duration: duration})
	}
	return records, nil
}

// Calcula el promedio de duración para cada tarea en un conjunto de registros
func calculateAverages(records []Record) map[string]float64 {
	averages := make(map[string]float64)
	counts := make(map[string]int)

	for _, record := range records {
		averages[record.Task] += record.Duration
		counts[record.Task]++
	}

	for task, total := range averages {
		averages[task] = total / float64(counts[task]) // Calcula el promedio
	}

	return averages
}

// Obtiene uso de CPU y memoria
func getSystemUsage(c *gin.Context) {
	cpuUsage, _ := cpu.Percent(time.Second, false)
	memory, _ := mem.VirtualMemory()
	data := gin.H{
		"cpu":    cpuUsage[0],
		"memory": memory.UsedPercent,
	}
	c.JSON(http.StatusOK, data)
}

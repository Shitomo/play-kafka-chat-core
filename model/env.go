package model

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

func LoadEnv(pathToEnv string) {
	err := godotenv.Load(pathToEnv + ".env")
	if err != nil {
		log.Fatalf("error loading .env file. caused by %v", err)
	}
}

func IsLocalEnv() bool {
	env := os.Getenv("ENV")
	return env == "local"
}

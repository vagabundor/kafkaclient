package scram

import (
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// Define hash generator functions
var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

// XDGSCRAMClient implements Sarama's SCRAMClient interface
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) error {
	log.Printf("Initializing SCRAM client for user: %s", userName)

	client, err := x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		log.Printf("Failed to create SCRAM client: %v", err)
		return err
	}
	x.Client = client

	x.ClientConversation = x.Client.NewConversation()
	if x.ClientConversation == nil {
		log.Println("Failed to start SCRAM conversation: conversation is nil")
		return fmt.Errorf("SCRAM conversation is nil")
	}

	log.Println("SCRAM client successfully initialized")
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (string, error) {
	response, err := x.ClientConversation.Step(challenge)
	if err != nil {
		log.Printf("SCRAM step failed: %v", err)
		return "", err
	}
	log.Println("SCRAM step succeeded")
	return response, nil
}

func (x *XDGSCRAMClient) Done() bool {
	if x.ClientConversation == nil {
		log.Println("SCRAM conversation is nil; returning false for Done()")
		return false
	}
	done := x.ClientConversation.Done()
	log.Printf("SCRAM conversation done: %v", done)
	return done
}

// XDGSCRAMClientGenerator generates XDGSCRAMClient instances for Sarama
type XDGSCRAMClientGenerator struct {
	HashGeneratorFcn scram.HashGeneratorFcn
}

// Generate creates a new SCRAM client instance for Sarama
func (x *XDGSCRAMClientGenerator) Generate() sarama.SCRAMClient {
	return &XDGSCRAMClient{
		HashGeneratorFcn: x.HashGeneratorFcn,
	}
}

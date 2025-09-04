package neo4j

import (
	"context"
	"fmt"
	neo4jtracing "github.com/raito-io/neo4j-tracing"
	"log"
	"os"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"

	"github.com/mitchellh/mapstructure"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

type TransactionOptions struct {
	RequiresNew bool
}

type TransactionRunner[T any] func(ctx context.Context) (*T, error)

type neo4jTransaction struct{}

var transaction = neo4jTransaction{}

var mut = sync.Mutex{}

func restartDriver(ctx context.Context, driver *neo4j.DriverWithContext) error {
	mut.Lock()

	defer mut.Unlock()

	connectionErr := (*driver).VerifyConnectivity(context.Background())

	if connectionErr == nil {
		return nil
	}

	err := (*driver).Close(ctx)

	if err != nil {
		return err
	}

	neo4jUri := os.Getenv("NEO4J_URI")
	neo4jUsername := os.Getenv("NEO4J_USERNAME")
	neo4jPassword := os.Getenv("NEO4J_PASSWORD")

	neo4jDriver, neo4jErr := NewDriver(neo4jUri, neo4jUsername, neo4jPassword)

	if neo4jErr != nil {
		log.Printf("error while connecting to neo4j %v", neo4jErr)
		return neo4jErr
	}

	*driver = *neo4jDriver

	return nil
}

func NewDriver(uri, username, password string) (*neo4j.DriverWithContext, error) {
	driverFactory := neo4jtracing.NewNeo4jTracer()
	driver, err := driverFactory.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""), func(config *config.Config) {
		config.ConnectionAcquisitionTimeout = 60 * time.Second
		config.SocketConnectTimeout = 60 * time.Second
		config.ConnectionLivenessCheckTimeout = 60 * time.Second
		config.NotificationsMinSeverity = notifications.DisabledLevel
		config.MaxConnectionPoolSize = 200
		config.MaxTransactionRetryTime = 120 * time.Second
	})

	if err != nil {
		return nil, err
	}

	if connectionErr := driver.VerifyConnectivity(context.Background()); connectionErr != nil {
		return nil, connectionErr
	}

	fmt.Println("Pinged your deployment. You successfully connected to Neo4j!")

	return &driver, nil
}

func WithNewSession(ctx context.Context, driver *neo4j.DriverWithContext) neo4j.SessionWithContext {
	session := (*driver).NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite,
		DatabaseName: "neo4j",
	})
	return session
}

var defaultOptions = TransactionOptions{
	RequiresNew: false,
}

func applyOptions(defaultOptions TransactionOptions, opts ...func(opt TransactionOptions)) {
	for _, apply := range opts {
		apply(defaultOptions)
	}
}

func WithTransaction[T any](driver *neo4j.DriverWithContext) func(ctx context.Context, runTransaction TransactionRunner[T], opts ...func(opt TransactionOptions)) (*T, error) {

	execute := func(ctx context.Context, runTransaction TransactionRunner[T], opts ...func(opt TransactionOptions)) (*T, error) {
		applyOptions(defaultOptions, opts...)

		tx, _ := ctx.Value(transaction).(neo4j.ExplicitTransaction)

		if tx != nil && !defaultOptions.RequiresNew {
			ctx = context.WithValue(ctx, transaction, tx)
			return runTransaction(ctx)
		}

		session := WithNewSession(ctx, driver)

		tx, beginTxErr := session.BeginTransaction(ctx)

		if beginTxErr != nil {
			return nil, beginTxErr
		}

		defer func() {

			err := tx.Close(ctx)
			if err != nil {
				log.Println(err)
			}

			err = session.Close(ctx)

			if err != nil {
				log.Println(err)
			}
		}()

		ctx = context.WithValue(ctx, transaction, tx)

		result, executionErr := runTransaction(ctx)

		if executionErr != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil {
				return nil, rollbackErr
			}
			return nil, executionErr
		}

		commitErr := tx.Commit(ctx)

		if commitErr != nil {
			return nil, commitErr
		}

		return result, nil
	}

	return func(ctx context.Context, runTransaction TransactionRunner[T], opts ...func(opt TransactionOptions)) (*T, error) {
		return handleSessionError[T](ctx, driver, func() (*T, error) {
			return execute(ctx, runTransaction, opts...)
		})
	}
}

func handleParams(params any) any {

	var result any

	switch properties := params.(type) {
	case map[string]any:
		var results = make(map[string]any)

		for key, property := range properties {
			results[key] = handleParams(property)
		}
		result = results
	case *time.Time:
		if properties != nil {
			result = *properties
		} else {
			result = nil
		}

	case []map[string]any:
		var results = make([]any, len(properties))

		for i, property := range properties {
			results[i] = handleParams(property)
		}
		result = results
	default:
		result = properties
	}

	return result
}

func StartQueryExecution(driver *neo4j.DriverWithContext) func(ctx context.Context, cypher string, params map[string]any) (func(), neo4j.ResultWithContext, error) {
	return func(ctx context.Context, cypher string, params map[string]any) (func(), neo4j.ResultWithContext, error) {

		params = handleParams(params).(map[string]any)

		tx, _ := ctx.Value(transaction).(neo4j.ExplicitTransaction)

		if tx != nil {
			result, err := tx.Run(ctx, cypher, params)
			return func() {}, result, err
		}

		newSession := WithNewSession(ctx, driver)

		result, err := newSession.Run(ctx, cypher, params)

		dispose := func() {
			err := newSession.Close(ctx)
			if err != nil {
				log.Println(err)
			}
		}

		return dispose, result, err
	}
}

func ExecuteQuery(ctx context.Context, driver *neo4j.DriverWithContext, query string, params map[string]any) (neo4j.ResultWithContext, error) {

	rs, err := handleSessionError[neo4j.ResultWithContext](ctx, driver, func() (*neo4j.ResultWithContext, error) {

		dispose, result, err := StartQueryExecution(driver)(ctx, query, params)

		defer dispose()

		return &result, err
	})

	return *rs, err
}

func handleSessionError[T any](ctx context.Context, driver *neo4j.DriverWithContext, doJob func() (*T, error)) (*T, error) {

	result, err := doJob()

	fmt.Printf("error: %v\n", err)

	if err != nil && neo4j.IsConnectivityError(err) {

		err := restartDriver(ctx, driver)

		if err != nil {
			return nil, err
		}

		result, err := doJob()

		if err != nil {
			return nil, err
		}

		return result, nil
	}

	return result, err
}

func mapField(node any) any {

	var result any

	switch properties := node.(type) {
	case dbtype.Node:
		var results = make(map[string]any)

		for key, property := range properties.Props {
			results[key] = mapField(property)
		}
		result = results
	case map[string]any:
		var results = make(map[string]any)

		for key, property := range properties {
			results[key] = mapField(property)
		}
		result = results
	case dbtype.LocalDateTime:
		result = properties.Time()
	case []any:
		var results = make([]any, len(properties))

		for i, property := range properties {
			results[i] = mapField(property)
		}
		result = results
	default:
		result = properties
	}

	return result
}

func mapResultsToSet[T any](ctx context.Context, result neo4j.ResultWithContext) ([]T, error) {

	var results = make([]T, 0)

	for result.Next(ctx) {
		record := result.Record()

		properties := mapField(record.Values[0])

		var newData T
		err := mapstructure.Decode(properties, &newData)

		if err != nil {
			return nil, err
		}

		results = append(results, newData)
	}

	return results, nil
}

func ExecuteWithMapping[T any](
	ctx context.Context,
	driver *neo4j.DriverWithContext,
	query string,
	params map[string]any,
) ([]T, error) {

	rs, err := handleSessionError[[]T](ctx, driver, func() (*[]T, error) {

		dispose, result, err := StartQueryExecution(driver)(ctx, query, params)

		defer dispose()

		if err != nil {
			return nil, err
		}

		data, err := mapResultsToSet[T](ctx, result)

		return &data, err
	})

	if err != nil {
		return nil, err
	}

	return *rs, err
}

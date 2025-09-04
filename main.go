package main

import (
	"context"
	"fmt"
	"go_neo4j_tx_manager/core/neo4j"
	"os"
)

func main() {

	neo4jUri := os.Getenv("NEO4J_URI")
	neo4jUser := os.Getenv("NEO4J_USER")
	neo4jPassword := os.Getenv("NEO4J_PASSWORD")

	neo4jDriver, err := neo4j.NewDriver(neo4jUri, neo4jUser, neo4jPassword)

	if err != nil {
		panic(err)
	}

	txManager := neo4j.WithTransaction[any](neo4jDriver)

	ctx := context.Background()

	result, err := txManager(ctx, func(ctx context.Context) (*any, error) {
		// do work that use transaction context

		// in example, all queries / writes run using transaction context

		neo4j.ExecuteQuery(ctx, neo4jDriver, "MATCH (n: NodeA) RETURN n", nil)

		neo4j.ExecuteQuery(ctx, neo4jDriver, "MATCH (x :NodeB) RETURN n", nil)

		neo4j.ExecuteQuery(ctx, neo4jDriver, "MATCH (n :NodeC) RETURN n", nil)

		query1 := `
			MATCH (na :NodeA)
			MATCH (nb :NodeB)
			MATCH (na)-[:RELATED_TO]->(nb)
			RETURN *
		`

		neo4j.ExecuteQuery(ctx, neo4jDriver, query1, nil)

		query2 := `
			MATCH (nb :NodeB)
			MATCH (nc :NodeC)
			MATCH (nb)-[:RELATED_TO]->(nc)
			RETURN *
		`

		neo4j.ExecuteQuery(ctx, neo4jDriver, query2, nil)

		return nil, nil
	})

	if err != nil {
		return
	}

	fmt.Print(result)
}

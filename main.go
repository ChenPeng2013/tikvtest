package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"google.golang.org/grpc"
	"github.com/satori/go.uuid"
)

var (
	addr = flag.String("tikv", "", "tikv grpc addr")
)

func main() {
	flag.Parse()
	fmt.Println(addr)
	
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	c := import_kvpb.NewImportKVClient(conn)
	ctx := context.Background()

	id := uuid.NewV4().Bytes()
	fmt.Println(id)
	_, err = c.OpenEngine(ctx, &import_kvpb.OpenEngineRequest{
		Uuid: []byte(id),
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		_, err := c.CloseEngine(ctx, &import_kvpb.CloseEngineRequest{Uuid: []byte(id)})
		if err != nil {
			fmt.Println(err)
		}
	}()
	
	ce, err := c.WriteEngine(ctx)
	if err != nil {
		panic(err)
	}

	//c.OpenEngine()
	wb := import_kvpb.WriteBatch{
		CommitTs: 124,
		Mutations: []*import_kvpb.Mutation{
			&import_kvpb.Mutation{
				Op: import_kvpb.Mutation_Put,
				Key: []byte("test"),
				Value: make([]byte, 1024 * 1024 * 100),
			},
		},
	}

	
	
	head := import_kvpb.WriteEngineRequest_Head{
		Head: &import_kvpb.WriteHead{Uuid: []byte(id)},
	}
	batch := import_kvpb.WriteEngineRequest_Batch{
		Batch: &wb,
	}

	wq := import_kvpb.WriteEngineRequest{Chunk: &head}
	if err := ce.Send(&wq); err != nil {
		panic(err)
	}

	wq = import_kvpb.WriteEngineRequest{Chunk: &batch}
	if err := ce.Send(&wq); err != nil {
		panic(err)
	}

	res, err := ce.CloseAndRecv()
	if err != nil {
		panic(err)
	}

	if res.Error != nil {
		panic(res.Error)
	}
	//
	//
	//let mut m = Mutation::new();
	//m.op = Mutation_OP::Put;
	//m.set_key(vec![2]);
	//m.set_value(vec![0; 90_000_000]);
	//let mut huge_batch = WriteBatch::new();
	//huge_batch.set_commit_ts(124);
	//huge_batch.mut_mutations().push(m);
	//let resp = send_write(&client, &head, &huge_batch).unwrap();
}

package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	wr "github.com/hunknownz/watchrelay"
	"github.com/hunknownz/watchrelay/resource"

	"github.com/sirupsen/logrus"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type Task struct {
	resource.Meta
	Uuid string
}

func produceTask(w *wr.WatchRelay, nTasks int) {
	// create task
	for i := 0; i < nTasks; i++ {
		task := &Task{
			Uuid: fmt.Sprintf("task-%d", i),
		}
		ctx := context.Background()
		err := wr.Create[*Task](w, ctx, nil, nil, task)
		if err != nil {
			logrus.Errorf("wr.Create %s", err)
			continue
		}
		time.Sleep(1 * time.Second)
	}
}

func consumeTask(w *wr.WatchRelay) {
	// consume task

	r := wr.Watch[*Task](w, context.Background(), nil, 0)

	for events := range r.Events {
		for _, event := range events {
			fmt.Printf("task: %+v\n", event.Value)
		}
	}
}

func initDatabase() (*gorm.DB, error) {
	gormConfig := &gorm.Config{
		SkipDefaultTransaction:                   true,
		DisableForeignKeyConstraintWhenMigrating: true,
		NamingStrategy: &schema.NamingStrategy{
			SingularTable: true,
		},
	}
	dsn := "root:ic@tcp(192.168.0.142:9501)/ic?charset=utf8mb4&tls=false&parseTime=True&loc=Local&timeout=4s"
	db, err := gorm.Open(mysql.Open(dsn), gormConfig)
	if err != nil {
		return nil, err
	}
	db.AutoMigrate(&Task{})

	return db, nil
}

func main() {
	db, err := initDatabase()
	if err != nil {
		logrus.Errorf("initDatabase %s", err)
		panic(err)
	}

	w, err := wr.NewWatchRelay(db)
	if err != nil {
		logrus.Errorf("wr.NewWatchRelay %s", err)
		panic(err)
	}
	err = wr.RegisterResource[*Task](w)
	if err != nil {
		logrus.Errorf("wr.RegisterResource %s", err)
		panic(err)
	}
	w.Start(context.Background())

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		produceTask(w, 10)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeTask(w)
	}()

	wg.Wait()

	// rev, events, err := wr.After[*Task](w, ctx, 0, 0)
	// if err != nil {
	// 	fmt.Printf("wr.After %s\n", err)
	// }
	// fmt.Printf("rev: %d, events: %+v\n", rev, events)
	// for _, event := range events {
	// 	fmt.Printf("user: %+v\n", event.Value)
	// }
}

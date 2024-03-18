package main

import (
	"context"
	"encoding/json"
	"fmt"

	wr "github.com/hunknownz/watchrelay"
	"github.com/sirupsen/logrus"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type User struct {
	wr.Meta
	Name string
}

func main() {
	gormConfig := &gorm.Config{
		SkipDefaultTransaction:                   true,
		DisableForeignKeyConstraintWhenMigrating: true,
		NamingStrategy: &schema.NamingStrategy{
			SingularTable: true,
		},
	}
	dsn := "root:N*y+L8503-_2vTdi4a@tcp(192.168.1.216:4000)/ictest?charset=utf8mb4&tls=false&parseTime=True&loc=Local&timeout=4s"
	db, err := gorm.Open(mysql.Open(dsn), gormConfig)
	if err != nil {
		logrus.Errorf("gorm.Open %s", dsn)
	}
	w, err := wr.NewWatchRelay(db)
	if err != nil {
		logrus.Errorf("wr.NewWatchRelay %s", err)
	}
	// users := []*User{
	// 	&User{
	// 		Name: "user3",
	// 	},
	// 	&User{
	// 		Name: "user4",
	// 	},
	// }
	ctx := context.Background()
	// err = wr.Create[*User](w, ctx, nil, nil, users...)
	// if err != nil {
	// 	logrus.Errorf("wr.CreateWithLog %s", err)
	// }

	rev, events, err := wr.After(w, ctx, "user", 0, 0)
	if err != nil {
		fmt.Printf("wr.After %s\n", err)
	}
	fmt.Printf("rev: %d, events: %+v\n", rev, events)
	for _, event := range events {
		u := &User{}
		_ = json.Unmarshal(event.Value, u)
		fmt.Printf("user: %+v\n", u)
	}
}

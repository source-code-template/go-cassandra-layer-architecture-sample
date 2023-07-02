package main

import (
	"context"
	"fmt"
	"github.com/core-go/config"
	"github.com/core-go/core"
	"github.com/core-go/log"
	mid "github.com/core-go/log/middleware"
	"github.com/gorilla/mux"
	"net/http"

	"go-service/internal/app"
)

func main() {
	var conf app.Config
	err := config.Load(&conf, "configs/config")
	if err != nil {
		panic(err)
	}

	r := mux.NewRouter()

	log.Initialize(conf.Log)
	r.Use(mid.BuildContext)
	logger := mid.NewLogger()
	if log.IsInfoEnable() {
		r.Use(mid.Logger(conf.MiddleWare, log.InfoFields, logger))
	}
	r.Use(mid.Recover(log.PanicMsg))

	err = app.Route(context.Background(), r, conf)
	if err != nil {
		panic(err)
	}
	fmt.Println(core.ServerInfo(conf.Server))
	if err = http.ListenAndServe(core.Addr(conf.Server.Port), r); err != nil {
		fmt.Println(err.Error())
	}
}

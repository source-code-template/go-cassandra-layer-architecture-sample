package app

import (
	"context"
	"github.com/core-go/cassandra"
	v "github.com/core-go/core/v10"
	"github.com/core-go/health"
	ch "github.com/core-go/health/cassandra"
	"github.com/core-go/log"
	"github.com/core-go/search/cassandra"
	"github.com/core-go/search/template"
	"github.com/core-go/search/template/xml"
	"github.com/gocql/gocql"
	"reflect"
	"time"

	"go-service/internal/handler"
	"go-service/internal/model"
	"go-service/internal/repository"
	"go-service/internal/service"
)

const (
	Keyspace = `masterdata`

	CreateKeyspace = `create keyspace if not exists masterdata with replication = {'class':'SimpleStrategy', 'replication_factor':1}`

	CreateTable = `
					create table if not exists users (
					id varchar,
					username varchar,
					email varchar,
					phone varchar,
					date_of_birth date,
					primary key (id)
	)`
)

type ApplicationContext struct {
	Health *health.Handler
	User   handler.UserPort
}

func NewApp(ctx context.Context, config Config) (*ApplicationContext, error) {
	// connect to the cluster
	cluster := gocql.NewCluster(config.Cql.PublicIp)
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.Timeout = time.Second * 1000
	cluster.ConnectTimeout = time.Second * 1000
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: config.Cql.UserName, Password: config.Cql.Password}
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	//defer session.Close()

	// create keyspaces
	err = session.Query(CreateKeyspace).Exec()
	if err != nil {
		return nil, err
	}

	//switch keyspaces
	session.Close()
	cluster.Keyspace = Keyspace
	session, err = cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()

	// create table
	err = session.Query(CreateTable).Exec()
	if err != nil {
		return nil, err
	}

	templates, err := template.LoadTemplates(xml.Trim, "configs/query.xml")
	if err != nil {
		return nil, err
	}
	logError := log.LogError
	validator := v.NewValidator()

	userType := reflect.TypeOf(model.User{})
	userQuery := query.NewBuilder("users", userType)
	userSearchBuilder, err := cassandra.NewSearchBuilder(cluster, userType, userQuery.BuildQuery)
	if err != nil {
		return nil, err
	}
	userRepository, err := repository.NewUserRepository(cluster, templates)
	if err != nil {
		return nil, err
	}
	userService := service.NewUserService(userRepository)
	userHandler := handler.NewUserHandler(userSearchBuilder.Search, userService, validator.Validate, logError)

	cqlChecker := ch.NewHealthChecker(cluster)
	healthHandler := health.NewHandler(cqlChecker)

	return &ApplicationContext{
		Health: healthHandler,
		User:   userHandler,
	}, nil
}

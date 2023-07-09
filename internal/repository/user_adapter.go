package repository

import (
	"context"
	"fmt"
	q "github.com/core-go/cassandra"
	"github.com/core-go/search/convert"
	"github.com/core-go/search/template"
	c "github.com/core-go/search/template/cassandra"
	"github.com/gocql/gocql"
	"reflect"

	. "go-service/internal/model"
)

type UserAdapter struct {
	Cluster       *gocql.ClusterConfig
	ModelType     reflect.Type
	JsonColumnMap map[string]string
	Keys          []string
	FieldsIndex   map[string]int
	Fields        string
	Schema        *q.Schema
	templates     map[string]*template.Template
}

func NewUserRepository(db *gocql.ClusterConfig, templates map[string]*template.Template) (*UserAdapter, error) {
	userType := reflect.TypeOf(User{})
	fieldsIndex, schema, jsonColumnMap, keys, _, fields, err := q.Init(userType)
	if err != nil {
		return nil, err
	}
	return &UserAdapter{Cluster: db, ModelType: userType, JsonColumnMap: jsonColumnMap, Keys: keys, Schema: schema, Fields: fields, FieldsIndex: fieldsIndex, templates: templates}, nil
}

func (m *UserAdapter) All(ctx context.Context) ([]User, error) {
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()
	query := fmt.Sprintf("select %s from users", m.Fields)
	rows := session.Query(query).Iter()
	var users []User
	err = q.ScanIter(rows, &users, m.FieldsIndex)
	return users, err
}

func (m *UserAdapter) Load(ctx context.Context, id string) (*User, error) {
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()
	query := session.Query(fmt.Sprintf("select %s from users where id = ?", m.Fields), id)
	rows := query.Iter()
	var users []User
	err = q.ScanIter(rows, &users, m.FieldsIndex)
	if err != nil {
		return nil, err
	}
	if len(users) > 0 {
		return &users[0], nil
	}
	return nil, nil
}

func (m *UserAdapter) Create(ctx context.Context, user *User) (int64, error) {
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	defer session.Close()
	query, params := q.BuildToInsert("users", user, m.Schema)
	err = session.Query(query, params...).Exec()
	return 1, err
}

func (m *UserAdapter) Update(ctx context.Context, user *User) (int64, error) {
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	defer session.Close()
	query, params := q.BuildToUpdate("users", user, m.Schema)
	err = session.Query(query, params...).Exec()
	return 1, err
}

func (m *UserAdapter) Patch(ctx context.Context, user map[string]interface{}) (int64, error) {
	colMap := q.JSONToColumns(user, m.JsonColumnMap)
	query, args := q.BuildToPatchWithVersion("users", colMap, m.Keys, "")
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	defer session.Close()
	err = session.Query(query, args...).Exec()
	return 1, err
}

func (m *UserAdapter) Delete(ctx context.Context, id string) (int64, error) {
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	defer session.Close()
	query := "delete from users where id = ?"
	err = session.Query(query, id).Exec()
	return 1, err
}

func (m *UserAdapter) Search(ctx context.Context, filter *UserFilter) ([]User, string, error) {
	var users []User
	if filter.Limit <= 0 {
		return users, "", nil
	}
	ftr := convert.ToMapWithFields(filter, m.Fields, &m.ModelType)
	query, params := c.Build(ftr, *m.templates["user"])
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return users, "", err
	}
	defer session.Close()
	nextPageToken, err := q.QueryWithPage(session, m.FieldsIndex, &users, filter.Limit, filter.Next, query, params...)
	return users, nextPageToken, err
}

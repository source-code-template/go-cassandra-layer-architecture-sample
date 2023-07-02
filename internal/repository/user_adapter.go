package repository

import (
	"context"
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
	Schema        *q.Schema
	templates     map[string]*template.Template
}

func NewUserRepository(db *gocql.ClusterConfig, templates map[string]*template.Template) (*UserAdapter, error) {
	userType := reflect.TypeOf(User{})
	jsonColumnMap := q.MakeJsonColumnMap(userType)
	keys, _ := q.FindPrimaryKeys(userType)
	fieldsIndex, err := q.GetColumnIndexes(userType)
	if err != nil {
		return nil, err
	}
	schema := q.CreateSchema(userType)
	return &UserAdapter{Cluster: db, ModelType: userType, JsonColumnMap: jsonColumnMap, Keys: keys, Schema: schema, FieldsIndex: fieldsIndex, templates: templates}, nil
}

func (m *UserAdapter) All(ctx context.Context) ([]User, error) {
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	query := "select * from users"
	rows := session.Query(query).Iter()
	var users []User
	err = q.ScanIter(rows, users, m.FieldsIndex)
	return users, err
}

func (m *UserAdapter) Load(ctx context.Context, id string) (*User, error) {
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	query := session.Query("select * from users where id = ?", id)
	rows := query.Iter()
	var users []User
	err = q.ScanIter(rows, users, m.FieldsIndex)
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
	query, params := q.BuildToInsert("users", user, m.Schema)
	err = session.Query(query, params...).Exec()
	if err != nil {
		return -1, nil
	}
	return 1, nil
}

func (m *UserAdapter) Update(ctx context.Context, user *User) (int64, error) {
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	query, params := q.BuildToUpdate("users", user, m.Schema)
	err = session.Query(query, params...).Exec()
	if err != nil {
		return -1, err
	}
	return 1, nil
}

func (m *UserAdapter) Patch(ctx context.Context, user map[string]interface{}) (int64, error) {
	colMap := q.JSONToColumns(user, m.JsonColumnMap)
	query, args := q.BuildToPatchWithVersion("users", colMap, m.Keys, "")
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	err = session.Query(query, args...).Exec()
	if err != nil {
		return -1, err
	}
	return 1, nil
}

func (m *UserAdapter) Delete(ctx context.Context, id string) (int64, error) {
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	query := "delete from users where id = ?"
	err = session.Query(query, id).Exec()
	if err != nil {
		return -1, err
	}
	return 1, nil
}

func (m *UserAdapter) Search(ctx context.Context, filter *UserFilter) ([]User, string, error) {
	var users []User
	if filter.Limit <= 0 {
		return users, "", nil
	}
	ftr := convert.ToMap(filter, &m.ModelType)
	query, params := c.Build(ftr, *m.templates["user"], q.BuildParam)
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return users, "", err
	}
	nextPageToken, err := q.QueryWithPage(session, m.FieldsIndex, &users, query, params, int(filter.Limit), filter.NextPageToken)
	return users, nextPageToken, err
}

package service

import (
	"context"

	. "go-service/internal/model"
	. "go-service/internal/repository"
)

func NewUserService(repository UserRepository) *UserUsecase {
	return &UserUsecase{repository: repository}
}

type UserUsecase struct {
	repository UserRepository
}

func (s *UserUsecase) All(ctx context.Context) ([]User, error) {
	return s.repository.All(ctx)
}
func (s *UserUsecase) Load(ctx context.Context, id string) (*User, error) {
	return s.repository.Load(ctx, id)
}
func (s *UserUsecase) Create(ctx context.Context, user *User) (int64, error) {
	return s.repository.Create(ctx, user)
}
func (s *UserUsecase) Update(ctx context.Context, user *User) (int64, error) {
	return s.repository.Update(ctx, user)
}
func (s *UserUsecase) Patch(ctx context.Context, user map[string]interface{}) (int64, error) {
	return s.repository.Patch(ctx, user)
}
func (s *UserUsecase) Delete(ctx context.Context, id string) (int64, error) {
	return s.repository.Delete(ctx, id)
}
func (s *UserUsecase) Search(ctx context.Context, filter *UserFilter) ([]User, string, error) {
	return s.repository.Search(ctx, filter)
}

package service

import (
	"context"

	pb "cwxu-algo/api/user/v1"
)

type AuthService struct {
	pb.UnimplementedAuthServer
}

func NewAuthService() *AuthService {
	return &AuthService{}
}

func (s *AuthService) Login(ctx context.Context, req *pb.LoginReq) (*pb.LoginRes, error) {
	return &pb.LoginRes{
		Success:  true,
		JwtToken: req.Password,
	}, nil
}

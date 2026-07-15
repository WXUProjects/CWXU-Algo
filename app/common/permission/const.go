package permission

// 角色 ID（注意：数值大小 ≠ 权限高低，Admin=1 却是最高权限）
const (
	RoleUser  = 0 // 普通用户
	RoleAdmin = 1 // 管理员（最高权限）
	RoleCoach = 2 // 教练
)

// RoleName 角色名称映射
var RoleName = map[int]string{
	RoleUser:  "普通用户",
	RoleCoach: "教练",
	RoleAdmin: "管理员",
}

// RoleRank 权限序：数值越大权限越高（勿直接比较 RoleID）
func RoleRank(role int) int {
	switch role {
	case RoleAdmin:
		return 100
	case RoleCoach:
		return 50
	case RoleUser:
		return 0
	default:
		return -1
	}
}

// String 获取角色名称
func String(role int) string {
	if name, ok := RoleName[role]; ok {
		return name
	}
	return "未知角色"
}

// IsValid 检查角色值是否合法
func IsValid(role int) bool {
	switch role {
	case RoleUser, RoleCoach, RoleAdmin:
		return true
	}
	return false
}

// CanManage 判断调用者是否能管理目标用户（权限序 >=）
func CanManage(callerRole, targetRole int) bool {
	return RoleRank(callerRole) >= RoleRank(targetRole)
}

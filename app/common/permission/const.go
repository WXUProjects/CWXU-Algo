package permission

// 角色 ID（注意：数值大小 ≠ 权限高低，Admin=1 却是最高权限）
// 0 队员 · 1 管理员 · 2 教练 · 3 队长
// 教练：管理端，无队员侧个人资料流程
// 队长：教练管理权限 + 队员交题/资料
// 管理员：全部功能
const (
	RoleUser    = 0 // 队员
	RoleAdmin   = 1 // 管理员（最高权限）
	RoleCoach   = 2 // 教练（仅管理）
	RoleCaptain = 3 // 队长（教练 + 队员）
)

// RoleName 角色名称映射
var RoleName = map[int]string{
	RoleUser:    "队员",
	RoleAdmin:   "管理员",
	RoleCoach:   "教练",
	RoleCaptain: "队长",
}

// RoleRank 权限序：数值越大权限越高（勿直接比较 RoleID）
// 教练与队长同级管理权限；管理员最高
func RoleRank(role int) int {
	switch role {
	case RoleAdmin:
		return 100
	case RoleCoach, RoleCaptain:
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
	case RoleUser, RoleAdmin, RoleCoach, RoleCaptain:
		return true
	}
	return false
}

// IsStaff 是否具备管理端权限（管理员 / 教练 / 队长）
func IsStaff(role int) bool {
	return RoleRank(role) >= RoleRank(RoleCoach)
}

// IsMemberLike 是否具备队员侧能力（交题、个人资料等）：队员 / 队长 / 管理员
// 纯教练不走队员资料流程
func IsMemberLike(role int) bool {
	switch role {
	case RoleUser, RoleCaptain, RoleAdmin:
		return true
	default:
		return false
	}
}

// CanManage 判断调用者是否能管理目标用户（权限序 >=）
func CanManage(callerRole, targetRole int) bool {
	return RoleRank(callerRole) >= RoleRank(targetRole)
}

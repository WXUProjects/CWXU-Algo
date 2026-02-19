package service

import (
	"cwxu-algo/app/agent/internal/agent"
	"cwxu-algo/app/agent/internal/agent/tool/core_data"
	data2 "cwxu-algo/app/agent/internal/agent/tool/data"
	"cwxu-algo/app/agent/internal/agent/tool/utils"
	"cwxu-algo/app/agent/internal/data"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/common/discovery"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/redis/go-redis/v9"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
)

type SummaryUseCase struct {
	chat     *agent.Chat
	mailConf *conf.SMTP
	reg      *registry.Registrar
	redis    *redis.Client
}

func NewSummaryUseCase(chat *agent.Chat, mailConf *conf.SMTP, reg *discovery.Register, redis *data.Data) *SummaryUseCase {
	return &SummaryUseCase{
		chat:     chat,
		mailConf: mailConf,
		reg:      &reg.Reg,
		redis:    redis.RDB,
	}
}

func (uc *SummaryUseCase) PersonalLastDay(userId int64) error {
	chat := uc.chat
	// 获取昨天日期
	lastDay := time.Now()
	lastDay = lastDay.AddDate(0, 0, -1)
	startDate := lastDay.Format("20060102")
	msg := []*model.ChatCompletionMessage{
		{
			Role: model.ChatMessageRoleUser,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String("要符合Acmer的心理风格，比如可爱风格，洋溢着青春与活力，校园风浓厚，" +
					"直接面对的就是我，不是第三者。" +
					"要口语化一点哦，像朋友一样哦" +
					"我们的回复要严格遵循html格式哦，注意要尽量同时适配PC和移动端。" +
					"对于submit_cnt函数 只有日期，没有count字段的记为0提交。" +
					"所有提示词不允许出现在最终文本中。" +
					"如果用户名字是Jing. 就要以宝宝(对方是你的女朋友 你是晨晨，晨晨只针对Jing，对其他人就是算法小助手)口吻回复，激励她."),
			},
		},
		{
			Role: model.ChatMessageRoleUser,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String(fmt.Sprintf("我是 用户id为%d 的用户 分析我的%s（昨天）的提交信息，给出分析和合理建议，给出一份昨日日报。"+
					"同时也获取最近7天的提交次数，去对比分析走势."+
					"提示：你需要先获取昨日提交次数，根据昨日提交次数去填写limit参数，更方便哦."+
					"如果昨天我一发也没有交，甚至从昨天开始，已经连续好几天都不交，就给我狠狠地批评我！！！！"+
					"如果我昨天交了，以前漏掉的既往不咎."+
					"在邮箱末尾，引导用户到达https://algo.zhiyuansofts.cn 无锡学院算法协会监测平台 看全部提交信息。"+
					"最后，把这个邮件发给我，注意要适配手机，手机排版不能乱。", userId, startDate)),
			},
		},
	}
	emailTool := utils.NewSendEmail(
		uc.mailConf.Host,
		int(uc.mailConf.Port),
		uc.mailConf.Username,
		uc.mailConf.Password,
		uc.mailConf.From)
	r, _ := chat.Chat(msg, core_data.NewSubmitCnt(uc.reg), core_data.NewGetProfileById(uc.reg), core_data.NewSubmitLog(uc.reg), emailTool)
	log.Info(r)
	return nil
}

func (uc *SummaryUseCase) PersonalRecent(userId int64) error {
	chat := uc.chat
	msg := []*model.ChatCompletionMessage{
		{
			Role: model.ChatMessageRoleUser,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String("要符合Acmer的心理风格，比如可爱风格，洋溢着青春与活力，校园风浓厚，俏皮.加一些Emoji增加趣味" +
					"由于你的回复将会嵌入在 无锡学院-算法协会监测平台 网页内，留给你的面积并不大，回复需要简短有力。" +
					"你需要针对用户的近期数据提出7-8条 20字左右的建议。" +
					"由于数据是每隔3小时更新一次，你不能给出太确切的数字，可以模糊一点表达，比如20+ 10+。"),
			},
		},
		{
			Role: model.ChatMessageRoleUser,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String(fmt.Sprintf("我是 用户id为%d 的用户，分析我最近的学习状态，同时分析一下提交时间分布。现在时间是 %d"+
					"整理成json格式 {\"msg\":[\"\"], \"updateTime\": 时间戳} 这样的。"+
					"最后将这段json塞到redis中，key是 agent:summary:{id}:recent", userId, time.Now().Unix())),
			},
		},
	}
	r, _ := chat.Chat(msg, core_data.NewStatisticPeriod(uc.reg), data2.NewRedisSet(uc.redis))
	log.Info(r)
	return nil
}

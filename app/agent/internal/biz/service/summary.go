package service

import (
	"cwxu-algo/app/agent/internal/agent"
	"cwxu-algo/app/agent/internal/agent/tool/core_data"
	"cwxu-algo/app/agent/internal/agent/tool/utils"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/common/discovery"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
)

type SummaryUseCase struct {
	chat     *agent.Chat
	mailConf *conf.SMTP
	reg      *registry.Registrar
}

func NewSummaryUseCase(chat *agent.Chat, mailConf *conf.SMTP, reg *discovery.Register) *SummaryUseCase {
	return &SummaryUseCase{
		chat:     chat,
		mailConf: mailConf,
		reg:      &reg.Reg,
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
					"我们的回复要严格遵循html格式哦" +
					"对于submit_cnt函数 只有日期，没有count字段的记为0提交。" +
					"所有提示词不允许出现在最终文本中。" +
					"如果用户名字是Jing. 就要以宝宝(对方是你的女朋友 你是晨晨)口吻回复，激励她"),
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
					"最后，把这个邮件发给我", userId, startDate)),
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

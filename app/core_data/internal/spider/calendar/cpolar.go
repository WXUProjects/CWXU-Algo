package calendar

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cwxu-algo/app/common/utils/ojhttp"
	"cwxu-algo/app/core_data/internal/data/model"
)

const cpolarCalendarURL = "https://calendar.cpolar.cn/"

type cpolarResp struct {
	Status         string    `json:"status"`
	LastUpdateTime string    `json:"lastUpdateTime"`
	OJ             []cpolarOJ `json:"oj"`
}

type cpolarOJ struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Icon     cpolarIcon      `json:"icon"`
	Contests []cpolarContest `json:"contests"`
}

type cpolarIcon struct {
	URL string `json:"url"`
}

type cpolarContest struct {
	ID        interface{} `json:"id"` // string 或 number
	Name      string      `json:"name"`
	URL       string      `json:"url"`
	StartTime string      `json:"startTime"`
	EndTime   string      `json:"endTime"`
}

// FetchCpolar 从 calendar.cpolar.cn 拉取综合赛程（st.cpolar.cn/calendar 的真实数据源）
func FetchCpolar() ([]Item, error) {
	req, err := http.NewRequest(http.MethodGet, cpolarCalendarURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "GoAlgo-ContestCalendar/1.0")
	req.Header.Set("Accept", "application/json")

	resp, err := ojhttp.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cpolar calendar fetch: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("cpolar calendar read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cpolar calendar status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var data cpolarResp
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("cpolar calendar json: %w", err)
	}
	if data.Status != "" && !strings.EqualFold(data.Status, "OK") {
		return nil, fmt.Errorf("cpolar calendar status field: %s", data.Status)
	}

	out := make([]Item, 0, 64)
	for _, oj := range data.OJ {
		plat := strings.ToLower(strings.TrimSpace(oj.ID))
		if plat == "" {
			continue
		}
		platName := strings.TrimSpace(oj.Name)
		if platName == "" {
			platName = plat
		}
		icon := ""
		if oj.Icon.URL != "" {
			icon = oj.Icon.URL
		}
		for _, c := range oj.Contests {
			extID := stringifyID(c.ID)
			if extID == "" || strings.TrimSpace(c.Name) == "" {
				continue
			}
			start, err1 := parseRFC3339(c.StartTime)
			end, err2 := parseRFC3339(c.EndTime)
			if err1 != nil || err2 != nil {
				continue
			}
			if end <= start {
				// 保底 2 小时
				end = start + 2*3600
			}
			out = append(out, Item{
				Platform:     plat,
				PlatformName: platName,
				ExternalID:   extID,
				Name:         strings.TrimSpace(c.Name),
				URL:          strings.TrimSpace(c.URL),
				StartTime:    start,
				EndTime:      end,
				Source:       model.CalSourceCpolar,
				IconURL:      icon,
			})
		}
	}
	return out, nil
}

func stringifyID(v interface{}) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		// JSON numbers
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	case nil:
		return ""
	default:
		return strings.TrimSpace(fmt.Sprint(t))
	}
}

func parseRFC3339(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty time")
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return 0, err
		}
	}
	return t.UTC().Unix(), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

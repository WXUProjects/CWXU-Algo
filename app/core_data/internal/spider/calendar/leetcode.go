package calendar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"cwxu-algo/app/common/utils/ojhttp"
	"cwxu-algo/app/core_data/internal/data/model"
)

const (
	leetcodeGraphQLURL     = "https://leetcode.cn/graphql"
	leetcodeContestListURL = "https://leetcode.cn/contest/api/list/"
)

// FetchLeetCode 优先 GraphQL（轻量），失败再降级 REST list
func FetchLeetCode() ([]Item, error) {
	if items, err := fetchLeetCodeGraphQL(); err == nil && len(items) > 0 {
		return items, nil
	} else if err != nil {
		// fall through
		_ = err
	}
	return fetchLeetCodeREST()
}

func fetchLeetCodeGraphQL() ([]Item, error) {
	payload := []byte(`{"query":"query{contestUpcomingContests{title titleSlug startTime duration isVirtual}}"}`)
	req, err := http.NewRequest(http.MethodPost, leetcodeGraphQLURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; GoAlgo-ContestCalendar/1.0)")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Referer", "https://leetcode.cn/contest/")

	resp, err := ojhttp.Do(req)
	if err != nil {
		return nil, fmt.Errorf("leetcode graphql: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("leetcode graphql read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("leetcode graphql status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var data struct {
		Data struct {
			ContestUpcomingContests []struct {
				Title     string `json:"title"`
				TitleSlug string `json:"titleSlug"`
				StartTime int64  `json:"startTime"`
				Duration  int64  `json:"duration"`
				IsVirtual bool   `json:"isVirtual"`
			} `json:"contestUpcomingContests"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("leetcode graphql json: %w", err)
	}
	if len(data.Errors) > 0 {
		return nil, fmt.Errorf("leetcode graphql: %s", data.Errors[0].Message)
	}

	out := make([]Item, 0, len(data.Data.ContestUpcomingContests))
	for _, c := range data.Data.ContestUpcomingContests {
		if c.IsVirtual {
			continue
		}
		slug := strings.TrimSpace(c.TitleSlug)
		if slug == "" || c.StartTime <= 0 {
			continue
		}
		dur := c.Duration
		if dur <= 0 {
			dur = 5400
		}
		title := strings.TrimSpace(c.Title)
		if title == "" {
			title = slug
		}
		out = append(out, Item{
			Platform:     NormalizePlatform("leetcode"),
			PlatformName: "力扣",
			ExternalID:   slug,
			Name:         title,
			URL:          "https://leetcode.cn/contest/" + slug,
			StartTime:    c.StartTime,
			EndTime:      c.StartTime + dur,
			Source:       model.CalSourceLeetCode,
			IconURL:      "https://leetcode.cn/favicon.ico",
		})
	}
	return out, nil
}

func fetchLeetCodeREST() ([]Item, error) {
	req, err := http.NewRequest(http.MethodGet, leetcodeContestListURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; GoAlgo-ContestCalendar/1.0)")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Referer", "https://leetcode.cn/contest/")

	resp, err := ojhttp.Do(req)
	if err != nil {
		return nil, fmt.Errorf("leetcode contest list: %w", err)
	}
	defer resp.Body.Close()
	// list API 可能带超长 description，放宽到 16MB
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("leetcode contest read: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("leetcode contest status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var data struct {
		Contests []struct {
			ID        int64  `json:"id"`
			Title     string `json:"title"`
			TitleSlug string `json:"title_slug"`
			Duration  int64  `json:"duration"`
			StartTime int64  `json:"start_time"`
			IsVirtual bool   `json:"is_virtual"`
			IsPrivate bool   `json:"is_private"`
		} `json:"contests"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("leetcode contest json: %w", err)
	}

	out := make([]Item, 0, len(data.Contests))
	for _, c := range data.Contests {
		if c.IsVirtual || c.IsPrivate {
			continue
		}
		slug := strings.TrimSpace(c.TitleSlug)
		if slug == "" || c.StartTime <= 0 {
			continue
		}
		dur := c.Duration
		if dur <= 0 {
			dur = 5400
		}
		title := strings.TrimSpace(c.Title)
		if title == "" {
			title = slug
		}
		out = append(out, Item{
			Platform:     NormalizePlatform("leetcode"),
			PlatformName: "力扣",
			ExternalID:   slug,
			Name:         title,
			URL:          "https://leetcode.cn/contest/" + slug,
			StartTime:    c.StartTime,
			EndTime:      c.StartTime + dur,
			Source:       model.CalSourceLeetCode,
			IconURL:      "https://leetcode.cn/favicon.ico",
		})
	}
	return out, nil
}

// FetchAll 串行拉取两源，单源失败不阻断另一源
func FetchAll() (items []Item, errs []error) {
	if c, err := FetchCpolar(); err != nil {
		errs = append(errs, err)
	} else {
		items = append(items, c...)
	}
	if l, err := FetchLeetCode(); err != nil {
		errs = append(errs, err)
	} else {
		items = append(items, l...)
	}
	return items, errs
}

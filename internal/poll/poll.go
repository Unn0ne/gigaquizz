package poll

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrNotFound = errors.New("poll not found")
	ErrOverlap  = errors.New("another poll is scheduled in this time window")
)

type Option struct {
	ID    int    `json:"id"`
	Label string `json:"label"`
}

type Poll struct {
	ID          string     `json:"id"`
	Question    string     `json:"question"`
	Type        string     `json:"type"`
	Options     []Option   `json:"options"`
	StartsAt    time.Time  `json:"starts_at"`
	EndsAt      time.Time  `json:"ends_at"`
	CreatedAt   time.Time  `json:"created_at"`
	FinalizedAt *time.Time `json:"finalized_at,omitempty"`
}

func (p Poll) State(now time.Time) string {
	if p.FinalizedAt != nil {
		return "final"
	}
	if now.Before(p.StartsAt) {
		return "scheduled"
	}
	if now.Before(p.EndsAt) {
		return "open"
	}
	return "processing"
}

type CreateInput struct {
	Question string     `json:"question"`
	Type     string     `json:"type"`
	Options  []string   `json:"options"`
	StartsAt *time.Time `json:"starts_at,omitempty"`
}

func (in *CreateInput) Validate() error {
	in.Question = strings.TrimSpace(in.Question)
	if n := utf8.RuneCountInString(in.Question); n < 1 || n > 300 {
		return errors.New("question must contain 1–300 characters")
	}
	if in.Type != "ab" && in.Type != "single" && in.Type != "multiple" {
		return errors.New("type must be ab, single or multiple")
	}
	if len(in.Options) < 2 || len(in.Options) > 20 {
		return errors.New("provide 2–20 options")
	}
	if in.Type == "ab" && len(in.Options) != 2 {
		return errors.New("A/B polls require exactly two options")
	}
	seen := make(map[string]bool)
	for i := range in.Options {
		in.Options[i] = strings.TrimSpace(in.Options[i])
		if n := utf8.RuneCountInString(in.Options[i]); n < 1 || n > 100 {
			return errors.New("each option must contain 1–100 characters")
		}
		key := strings.ToLower(in.Options[i])
		if seen[key] {
			return errors.New("option labels must be distinct")
		}
		seen[key] = true
	}
	return nil
}

func NormalizeChoices(choices []int) ([]int, error) {
	if len(choices) == 0 || len(choices) > 20 {
		return nil, errors.New("select 1–20 options")
	}
	result := slices.Clone(choices)
	slices.Sort(result)
	for i, v := range result {
		if v < 1 || v > 20 {
			return nil, fmt.Errorf("invalid option id")
		}
		if i > 0 && result[i-1] == v {
			return nil, errors.New("duplicate option ids")
		}
	}
	return result, nil
}

type Receipt struct {
	Status     string     `json:"status"`
	Choices    []int      `json:"choices,omitempty"`
	AcceptedAt *time.Time `json:"accepted_at,omitempty"`
}

type OptionCount struct {
	ID    int    `json:"id"`
	Label string `json:"label"`
	Votes int64  `json:"votes"`
}

type Results struct {
	PollID       string        `json:"poll_id"`
	State        string        `json:"state"`
	TotalVotes   int64         `json:"total_votes"`
	Options      []OptionCount `json:"options"`
	CalculatedAt time.Time     `json:"calculated_at"`
}

type Repository interface {
	Create(context.Context, CreateInput) (Poll, error)
	Get(context.Context, string) (Poll, error)
	List(context.Context) ([]Poll, error)
	Vote(context.Context, string, string, []int) (Receipt, error)
	Results(context.Context, string) (Results, error)
	FinalizeDue(context.Context) (int, error)
	Ping(context.Context) error
}

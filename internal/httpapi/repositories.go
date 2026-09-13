package httpapi

import (
	"context"

	"gigaquizz/internal/poll"
)

// HTTP consumes only request-facing capabilities. Lifecycle, finalization and
// ownership stay with the application; the full historical poll.Repository
// remains available to its existing consumers.
type PublicRepository interface {
	Get(context.Context, string) (poll.Poll, error)
	Vote(context.Context, string, string, []int) (poll.Receipt, error)
}
type AdminRepository interface {
	Create(context.Context, poll.CreateInput) (poll.Poll, error)
	List(context.Context) ([]poll.Poll, error)
	Results(context.Context, string) (poll.Results, error)
}

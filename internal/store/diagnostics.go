package store

type Diagnostics struct {
	Subscriptions int `json:"subscriptions"`
	Events        int `json:"events"`
	Pending       int `json:"pending"`
	DeadLetters   int `json:"dead_letters"`
}

func (s *Store) Diagnostics() (Diagnostics, error) {
	subs, err := s.ListSubscriptions()
	if err != nil {
		return Diagnostics{}, err
	}
	events, err := s.ListEvents(10000)
	if err != nil {
		return Diagnostics{}, err
	}
	pending, err := s.PendingAttempts()
	if err != nil {
		return Diagnostics{}, err
	}
	dead, err := s.ListDeadLetters()
	if err != nil {
		return Diagnostics{}, err
	}
	return Diagnostics{Subscriptions: len(subs), Events: len(events), Pending: len(pending), DeadLetters: len(dead)}, nil
}

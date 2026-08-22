package othertickets

type LocalTicket struct {
	Reference string `json:"reference"`
	Priority  int    `json:"priority"`
}

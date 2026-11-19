package store

import "time"

// Message is a GORM model representing a single message row.
type Message struct {
	ID        string    `gorm:"primaryKey;type:text" json:"id"`
	Body      string    `gorm:"type:text;not null" json:"body"`
	Origin    string    `gorm:"type:text;not null" json:"origin"`
	CreatedAt time.Time `json:"created_at"`
}

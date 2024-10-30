package domain

import (
	"errors"
	"time"
)

var ErrInvalidResolution = errors.New("invalid resolution")

type Resolution time.Duration

func (r Resolution) String() string {
	return resolutionToString[r]
}

func ParseResolution(s string) (Resolution, error) {
	r, ok := stringToResolution[s]
	if !ok {
		return 0, ErrInvalidResolution
	}
	return r, nil
}

var resolutionToString = map[Resolution]string{
	Resolution(time.Second):              "s1",
	Resolution(time.Second * 5):          "s5",
	Resolution(time.Second * 10):         "s10",
	Resolution(time.Second * 15):         "s15",
	Resolution(time.Second * 30):         "s30",
	Resolution(time.Second * 45):         "s45",
	Resolution(time.Minute):              "m1",
	Resolution(time.Minute * 2):          "m2",
	Resolution(time.Minute * 3):          "m3",
	Resolution(time.Minute * 5):          "m5",
	Resolution(time.Minute * 10):         "m10",
	Resolution(time.Minute * 15):         "m15",
	Resolution(time.Minute * 30):         "m30",
	Resolution(time.Minute * 45):         "m45",
	Resolution(time.Hour):                "h1",
	Resolution(time.Hour * 2):            "h2",
	Resolution(time.Hour * 3):            "h3",
	Resolution(time.Hour * 4):            "h4",
	Resolution(time.Hour * 6):            "h6",
	Resolution(time.Hour * 8):            "h8",
	Resolution(time.Hour * 12):           "h12",
	Resolution(time.Hour * 24):           "d1",
	Resolution(time.Hour * 24 * 7):       "w1",
	Resolution(time.Hour * 24 * 30):      "M1",
	Resolution(time.Hour * 24 * 30 * 3):  "M3",
	Resolution(time.Hour * 24 * 30 * 4):  "M4",
	Resolution(time.Hour * 24 * 30 * 6):  "M6",
	Resolution(time.Hour * 24 * 30 * 12): "y1",
}

var stringToResolution = map[string]Resolution{
	"s1":  Resolution(time.Second),
	"s5":  Resolution(time.Second * 5),
	"s10": Resolution(time.Second * 10),
	"s15": Resolution(time.Second * 15),
	"s30": Resolution(time.Second * 30),
	"s45": Resolution(time.Second * 45),
	"m1":  Resolution(time.Minute),
	"m2":  Resolution(time.Minute * 2),
	"m3":  Resolution(time.Minute * 3),
	"m5":  Resolution(time.Minute * 5),
	"m10": Resolution(time.Minute * 10),
	"m15": Resolution(time.Minute * 15),
	"m30": Resolution(time.Minute * 30),
	"m45": Resolution(time.Minute * 45),
	"h1":  Resolution(time.Hour),
	"h2":  Resolution(time.Hour * 2),
	"h3":  Resolution(time.Hour * 3),
	"h4":  Resolution(time.Hour * 4),
	"h6":  Resolution(time.Hour * 6),
	"h8":  Resolution(time.Hour * 8),
	"h12": Resolution(time.Hour * 12),
	"d1":  Resolution(time.Hour * 24),
	"w1":  Resolution(time.Hour * 24 * 7),
	"M1":  Resolution(time.Hour * 24 * 30),
	"M3":  Resolution(time.Hour * 24 * 30 * 3),
	"M4":  Resolution(time.Hour * 24 * 30 * 4),
	"M6":  Resolution(time.Hour * 24 * 30 * 6),
	"y1":  Resolution(time.Hour * 24 * 30 * 12),
}

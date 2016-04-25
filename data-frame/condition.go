package df

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"util"
)

type Condition interface {
	Compare(Cell, string) bool
	String() string
}

var reg *regexp.Regexp

func init() {
	var err error
	reg, err = regexp.Compile("^\\s*([^\\s><!=]+)\\s*([><!=]={0,1})\\s*(.+)\\s*$")
	util.IfErrPanic(err)
}

func NewCondition(conds []string) map[string]Condition {
	cs := make(map[string][]Condition)

	for _, cond := range conds {
		strs := reg.FindAllStringSubmatch(cond, -1)
		if len(strs) == 1 && len(strs[0]) == 4 {
			switch strs[0][2] {
			case "<":
				c := LtCondition(strs[0][3])
				cs[strs[0][1]] = append(cs[strs[0][1]], &c)
			case ">":
				c := GtCondition(strs[0][3])
				cs[strs[0][1]] = append(cs[strs[0][1]], &c)
			case ">=":
				c := NltCondition(strs[0][3])
				cs[strs[0][1]] = append(cs[strs[0][1]], &c)
			case "<=":
				c := NgtCondition(strs[0][3])
				cs[strs[0][1]] = append(cs[strs[0][1]], &c)
			case "==":
				c := EqCondition(strs[0][3])
				cs[strs[0][1]] = append(cs[strs[0][1]], &c)
			default:
				panic("invalid operation: " + strs[0][2])
			}
		}
	}

	rcs := make(map[string]Condition)
	for k, v := range cs {
		if len(v) == 1 {
			rcs[k] = v[0]
		} else {
			var ac ArrayCondition = ArrayCondition(v)
			rcs[k] = &ac
		}
	}

	return rcs
}

type EqCondition string

func (c EqCondition) String() string {
	return "EqCondition:" + string(c)
}

func (c EqCondition) Compare(v Cell, t string) bool {
	switch t {
	case "df.String":
		return strings.Compare(string(c), v.String()) == 0
	case "df.Int":
		if i, err := strconv.Atoi(string(c)); err == nil {
			iv, _ := v.Int()
			return i == *iv
		} else {
			return false
		}
	case "df.Bool":
		b, _ := v.Bool()
		if *b {
			return string(c) == "true"
		} else {
			return string(c) == "false"
		}
	case "df.Float":
		if f, err := strconv.ParseFloat(string(c), 64); err != nil {
			fv, _ := v.Float()
			return math.Abs(*fv-f) < 0.0001
		} else {
			return false
		}
	default:
		return false
	}
}

type GtCondition string

func (c GtCondition) String() string {
	return "GtCondition" + string(c)
}

func (c GtCondition) Compare(v Cell, t string) bool {
	switch t {
	case "df.String":
		return strings.Compare(v.String(), string(c)) > 0
	case "df.Int":
		if i, err := strconv.Atoi(string(c)); err == nil {
			iv, _ := v.Int()
			return i > *iv
		} else {
			return false
		}
	case "df.Float":
		if f, err := strconv.ParseFloat(string(c), 64); err != nil {
			fv, _ := v.Float()
			return f > *fv
		} else {
			return false
		}
	default:
		return false
	}
}

type LtCondition string

func (c LtCondition) String() string {
	return "LtCondition" + string(c)
}

func (c LtCondition) Compare(v Cell, t string) bool {
	switch t {
	case "df.String":
		return strings.Compare(v.String(), string(c)) < 0
	case "df.Int":
		if i, err := strconv.Atoi(string(c)); err == nil {
			iv, _ := v.Int()
			return i < *iv
		} else {
			return false
		}
	case "df.Float":
		if f, err := strconv.ParseFloat(string(c), 64); err != nil {
			fv, _ := v.Float()
			return f < *fv
		} else {
			return false
		}
	default:
		return false
	}
}

type NgtCondition string

func (c NgtCondition) String() string {
	return "NgtCondition:" + string(c)
}

func (c NgtCondition) Compare(v Cell, t string) bool {
	return LtCondition(c).Compare(v, t) || EqCondition(c).Compare(v, t)
}

type NltCondition string

func (c NltCondition) String() string {
	return "NltCondition:" + string(c)
}

func (c NltCondition) Compare(v Cell, t string) bool {
	return (GtCondition(c)).Compare(v, t) || (EqCondition(c)).Compare(v, t)
}

type ArrayCondition []Condition

func (c ArrayCondition) String() string {
	s := "{"
	for _, ic := range []Condition(c) {
		s = s + ic.String() + ","
	}
	s = s + "}"
	return s
}

func (c ArrayCondition) Compare(v Cell, t string) bool {
	for _, ic := range []Condition(c) {
		if !ic.Compare(v, t) {
			return false
		}
	}

	return true
}

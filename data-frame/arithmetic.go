package df

import (
	"errors"
)

func (d *DataFrame) DivColumn(a, b string) error {
	ca, oka := d.Columns[a]
	cb, okb := d.Columns[b]
	if !oka || !okb {
		return errors.New("column not find:" + a + " " + b)
	}

	switch {
	case ca.colType == "df.Int" && cb.colType == "df.Int":
		for i := 0; i < d.nRows; i++ {
			acell := ca.cells[i].(Int)
			bcell := cb.cells[i].(Int)

			av := *acell.i / *bcell.i
			acell.i = &av
		}
	case ca.colType == "df.Float" && cb.colType == "df.Int":
		for i := 0; i < d.nRows; i++ {
			acell := ca.cells[i].(Float)
			bcell := cb.cells[i].(Int)

			av := *acell.f / float64(*bcell.i)
			acell.f = &av
		}
	case ca.colType == "df.Int" && cb.colType == "df.Float":
		for i := 0; i < d.nRows; i++ {
			acell := ca.cells[i].(Int)
			bcell := cb.cells[i].(Float)

			av := int(float64(*acell.i) / *bcell.f)
			acell.i = &av
		}
	case ca.colType == "df.Float" && cb.colType == "df.Float":
		for i := 0; i < d.nRows; i++ {
			acell := ca.cells[i].(Float)
			bcell := cb.cells[i].(Float)

			av := *acell.f / *bcell.f
			acell.f = &av
		}
	default:
		return errors.New("types check fail")
	}

	return nil
}

func (d *DataFrame) DivValue(a string, v float64) error {
	ca, oka := d.Columns[a]
	if !oka {
		return errors.New("column not find:" + a)
	}

	switch {
	case ca.colType == "df.Int":
		for i := 0; i < d.nRows; i++ {
			acell := ca.cells[i].(Int)

			av := int(float64(*acell.i) / v)
			acell.i = &av
		}
	case ca.colType == "df.Float":
		for i := 0; i < d.nRows; i++ {
			acell := ca.cells[i].(Float)

			av := *acell.f / v
			acell.f = &av
		}
	default:
		return errors.New("types check fail")
	}

	return nil
}

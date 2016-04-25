package df

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// NOTE: The concept of NA is represented by nil pointers

// TODO: Constructors should allow options to set up:
//	DateFormat
//	TrimSpaces?

type rowable interface {
	String() string
}

// Cell is the interface that every cell in a DataFrame needs to comply with
type Cell interface {
	String() string
	Int() (*int, error)
	Float() (*float64, error)
	Bool() (*bool, error)
	NA() Cell
	IsNA() bool
	Checksum() [16]byte
	Copy() Cell
}

// Cells is a wrapper for a slice of Cells
type Cells []Cell

type tointeger interface {
	Int() (*int, error)
}

type tofloat interface {
	Float() (*float64, error)
}

type tobool interface {
	Bool() (*bool, error)
}

// DataFrame is the base data structure
type DataFrame struct {
	Columns   map[string]column
	colIndexs map[string]int
	nRows     int
}

// C represents a way to pass Colname and Elements to a DF constructor
type C struct {
	Colname  string
	Elements Cells
}

// T is used to represent the association between a column and it't type
type T map[string]string

// R represent a range from a number to another
type R struct {
	From int
	To   int
}

// u represents if an element is unique or if it appears on more than one place in
// addition to the index where it appears.
type u struct {
	unique  bool
	appears []int
}

//type Error struct {
//errorType Err
//}

//const defaultDateFormat = "2006-01-02"

// TODO: Implement a custom Error type that stores information about the type of
// error and the severity of it (Warning vs Error)
// Error types
//type Err int

//const (
//FormatError Err = iota
//UnknownType
//Warning
//Etc
//)

// TODO: Use enumns for type parsing declaration:
//   type parseType int
//   const (
//       String int = iota
//       Int
//       Float
//   )

// New is a constructor for DataFrames
func New(colConst ...C) (*DataFrame, error) {
	if len(colConst) == 0 {
		return nil, errors.New("Can't create empty DataFrame")
	}

	var colLength int
	df := &DataFrame{
		Columns:   map[string]column{},
		colIndexs: map[string]int{},
		nRows:     colLength,
	}
	for k, val := range colConst {
		col, err := newCol(val.Colname, val.Elements)
		if err != nil {
			return nil, err
		}

		// Check that the length of all columns are the same
		if k == 0 {
			colLength = len(col.cells)
		} else {
			if colLength != len(col.cells) {
				return nil, errors.New("columns don't have the same dimensions")
			}
		}
		df.Columns[val.Colname] = *col
		df.colIndexs[val.Colname] = k + 1
	}

	return df, nil
}

// Names is the getter method for the column names
func (df DataFrame) Names() []string {
	var names []string
	for _, v := range df.Columns {
		names = append(names, v.colName)
	}
	return names
}

func (df DataFrame) copy() DataFrame {
	columns := make(map[string]column)
	for k, v := range df.Columns {
		columns[k] = v.copy()
	}
	dfc := DataFrame{
		Columns:   columns,
		colIndexs: df.colIndexs,
		nRows:     df.nRows,
	}
	return dfc
}

// SetNames let us specify the column names of a DataFrame
func (df *DataFrame) SetNames(colnames []string) error {
	newcolumns := make(map[string]column)
	newindexes := make(map[string]int)

	for k, v := range df.colIndexs {
		if v < len(colnames) {
			if _, ok := newcolumns[colnames[v]]; ok {
				return errors.New("duplicate column name: " + colnames[v])
			}
			newcolumns[colnames[v]] = df.Columns[k]
			newindexes[colnames[v]] = v
		} else {
			newcolumns[k] = df.Columns[k]
			newindexes[k] = v
		}

		if col, ok := df.Columns[k]; ok {
			col.recountNumChars()
		}
	}
	df.Columns = newcolumns
	df.colIndexs = newindexes
	return nil
}

// LoadData will load the data from a multidimensional array of strings into
// a DataFrame object.
func (df *DataFrame) LoadData(records [][]string) error {
	// Calculate DataFrame dimensions
	nRows := len(records) - 1
	if nRows <= 0 {
		return errors.New("Empty dataframe")
	}
	colnames := records[0]
	nCols := len(colnames)

	// If colNames has empty elements we must fill it with unique colnames
	colnamesMap := make(map[string]bool)
	auxCounter := 0
	// Get unique columnenames
	for _, v := range colnames {
		if v != "" {
			if _, ok := colnamesMap[v]; !ok {
				colnamesMap[v] = true
			} else {
				return errors.New("Duplicated column names: " + v)
			}
		}
	}
	for k, v := range colnames {
		if v == "" {
			for {
				newColname := fmt.Sprint("V", auxCounter)
				auxCounter++
				if _, ok := colnamesMap[newColname]; !ok {
					colnames[k] = newColname
					colnamesMap[newColname] = true
					break
				}
			}
		}
	}

	// Generate a df to store the temporary values
	newDf := DataFrame{
		nRows:     nRows,
		Columns:   map[string]column{},
		colIndexs: map[string]int{},
	}

	// Fill the columns on the DataFrame
	for j := 0; j < nCols; j++ {
		colstrarr := []string{}
		for i := 1; i < nRows+1; i++ {
			colstrarr = append(colstrarr, records[i][j])
		}

		col, err := newCol(colnames[j], Strings(colstrarr))
		if err != nil {
			return err
		}
		newDf.Columns[colnames[j]] = *col
		newDf.colIndexs[colnames[j]] = j
	}

	*df = newDf
	return nil
}

func (df *DataFrame) LoadJson(jdata []map[string]interface{}) error {
	data := make([][]string, 1)
	index := make(map[string]int)

	for _, row := range jdata {
		rdata := make([]string, len(index))
		for k, v := range row {
			if idx, ok := index[k]; ok {
				rdata[idx] = fmt.Sprint(v)
			} else {
				index[k] = len(index)
				data[0] = append(data[0], k)
				for j := 1; j < len(data); j++ {
					data[j] = append(data[j], "")
				}
				rdata = append(rdata, fmt.Sprint(v))
			}
		}
		data = append(data, rdata)
	}

	return df.LoadData(data)
}

func (df *DataFrame) LoadCsv(data []byte) error {
	r := csv.NewReader(bytes.NewReader(data))
	records, err := r.ReadAll()
	if err != nil {
		return err
	}

	return df.LoadData(records)
}

// LoadData will load the data from a multidimensional array of strings into
// a DataFrame object.
func (df *DataFrame) LoadInterface(records [][]interface{}, colnames []string) error {
	// Calculate DataFrame dimensions
	nRows := len(records)
	if nRows <= 0 {
		return errors.New("Empty dataframe")
	}
	nCols := len(colnames)

	// If colNames has empty elements we must fill it with unique colnames
	colnamesMap := make(map[string]bool)
	auxCounter := 0
	// Get unique columnenames
	for _, v := range colnames {
		if v != "" {
			if _, ok := colnamesMap[v]; !ok {
				colnamesMap[v] = true
			} else {
				return errors.New("Duplicated column names: " + v)
			}
		}
	}
	for k, v := range colnames {
		if v == "" {
			for {
				newColname := fmt.Sprint("V", auxCounter)
				auxCounter++
				if _, ok := colnamesMap[newColname]; !ok {
					colnames[k] = newColname
					colnamesMap[newColname] = true
					break
				}
			}
		}
	}

	// Generate a df to store the temporary values
	newDf := DataFrame{
		nRows:     nRows,
		Columns:   map[string]column{},
		colIndexs: map[string]int{},
	}

	// Fill the columns on the DataFrame
	for j := 0; j < nCols; j++ {
		colstrarr := []string{}
		for i := 0; i < nRows; i++ {
			colstrarr = append(colstrarr, fmt.Sprint(records[i][j]))
		}

		col, err := newCol(colnames[j], Strings(colstrarr))
		if err != nil {
			return err
		}

		newDf.Columns[colnames[j]] = *col
		newDf.colIndexs[colnames[j]] = j
	}

	*df = newDf

	types := make([]string, len(colnames))
	for i := 0; i < nCols; i++ {
		types[i] = reflect.TypeOf(records[0][i]).String()
	}
	df.Parse(types)
	return nil
}

func (df *DataFrame) Parse(types interface{}) error {
	// Parse the DataFrame columns acording to the given types
	switch types.(type) {
	case []string:
		types := types.([]string)
		for k, v := range df.colIndexs {
			if v < len(types) {
				col := df.Columns[k].copy()
				err := col.ParseColumn(types[v])
				if err != nil {
					return nil
				}
				df.Columns[k] = col
			}
		}
	case T:
		types := types.(T)
		for k, v := range types {
			col := df.Columns[k].copy()
			err := col.ParseColumn(v)
			if err != nil {
				return err
			}

			df.Columns[k] = col
		}
	}

	return nil
}

// LoadAndParse will load the data from a multidimensional array of strings and
// parse it accordingly with the given types element. The types element can be
// a string array with matching dimensions to the number of columns or
// a DataFrame.T object.
func (df *DataFrame) LoadAndParse(records [][]string, types interface{}) error {
	// Initialize the DataFrame with all columns as string type
	err := df.LoadData(records)
	if err != nil {
		return err
	}
	return df.Parse(types)
}

// SaveRecords will save data to records in [][]string format
func (df DataFrame) SaveRecords() [][]string {
	if len(df.Columns) == 0 {
		return make([][]string, 0)
	}
	if df.nRows == 0 {
		records := make([][]string, 1)
		records[0] = df.Names()
		return records
	}

	var records [][]string

	records = append(records, df.Names())
	for i := 0; i < df.nRows; i++ {
		r := []string{}
		for _, v := range df.Columns {
			r = append(r, v.cells[i].String())
		}
		records = append(records, r)
	}

	return records
}

//// TODO: Save to other formats. JSON? XML?

// Dim will return the current dimensions of the DataFrame in a two element array
// where the first element is the number of rows and the second the number of
// columns.
func (df DataFrame) Dim() (dim [2]int) {
	dim[0] = df.nRows
	dim[1] = len(df.Columns)
	return
}

// NRows is the getter method for the number of rows in a DataFrame
func (df DataFrame) NRows() int {
	return df.nRows
}

// NCols is the getter method for the number of rows in a DataFrame
func (df DataFrame) NCols() int {
	return len(df.Columns)
}

// colIndex tries to find the column index for a given column name
func (df DataFrame) colIndex(colname string) (int, error) {
	if v, ok := df.colIndexs[colname]; ok {
		return v, nil
	}
	return 0, errors.New("Can't find the given column:")
}

// Subset will return a DataFrame that contains only the columns and rows contained
// on the given subset
func (df DataFrame) Subset(subsetCols interface{}, subsetRows interface{}) (*DataFrame, error) {
	dfA, err := df.SubsetColumns(subsetCols)
	if err != nil {
		return nil, err
	}
	dfB, err := dfA.SubsetRows(subsetRows)
	if err != nil {
		return nil, err
	}

	return dfB, nil
}

func (df DataFrame) DropColumn(col string) (*DataFrame, error) {
	var newcolnames []string
	for _, c := range df.Columns {
		if col != c.colName {
			newcolnames = append(newcolnames, col)
		}
	}

	return df.SubsetColumns(newcolnames)
}

// SubsetColumns will return a DataFrame that contains only the columns contained
// on the given subset
func (df DataFrame) SubsetColumns(subset interface{}) (*DataFrame, error) {
	// Generate a DataFrame to store the temporary values
	switch subset.(type) {
	case R:
		s := subset.(R)
		var cols []string
		for k, v := range df.colIndexs {
			if v >= s.From && v < s.To {
				cols = append(cols, k)
			}
		}
		return df.SubsetColumns(cols)
	case []int:
		colNums := subset.([]int)
		// Check for errors
		colNumsMap := make(map[int]bool)
		for _, v := range colNums {
			colNumsMap[v] = true
		}

		var cols []string
		for k, v := range df.colIndexs {
			if _, ok := colNumsMap[v]; ok {
				cols = append(cols, k)
			}
		}
		return df.SubsetColumns(cols)
	case []string:
		cols := subset.([]string)

		colindex := map[string]int{}
		for _, v := range cols {
			if idx, ok := df.colIndexs[v]; ok {
				colindex[v] = idx
			}
		}

		if len(colindex) == 0 {
			return nil, errors.New("Empty subset")
		}

		newDf := df.copy()
		for k, v := range df.colIndexs {
			if _, ok := colindex[k]; !ok {
				delete(newDf.Columns, k)

				for kk, vv := range colindex {
					if df.colIndexs[kk] > v {
						colindex[kk] = vv - 1
					}
				}
			}

		}
		return &newDf, nil

	default:
		return nil, errors.New("Unknown subsetting option")
	}
}

func (df DataFrame) FilterRows(colname string, f func(Cell) bool) (*DataFrame, error) {
	if col, ok := df.Columns[colname]; ok {
		var rows []int

		for i := 0; i < df.NRows(); i++ {
			if f(col.cells[i]) {
				rows = append(rows, i)
			}
		}

		return df.SubsetRows(rows)
	} else {
		return nil, errors.New(colname + " not exists")
	}
}

func (df DataFrame) ConditionRows(cs map[string]Condition) (*DataFrame, error) {
	if len(cs) == 0 {
		newDf := df.copy()
		return &newDf, nil
	}

	var rows []int

	for i := 0; i < df.NRows(); i++ {
		valid := true
		for k, c := range cs {
			if !c.Compare(df.Columns[k].cells[i], df.Columns[k].colType) {
				valid = false
				break
			}
		}

		if valid {
			rows = append(rows, i)
		}
	}

	return df.SubsetRows(rows)
}

// SubsetRows will return a DataFrame that contains only the selected rows
func (df DataFrame) SubsetRows(subset interface{}) (*DataFrame, error) {
	// Generate a DataFrame to store the temporary values
	newDf := df.copy()

	switch subset.(type) {
	case R:
		s := subset.(R)
		// Check for errors
		if s.From > s.To {
			return nil, errors.New("Bad subset: Start greater than Beginning")
		}
		if s.From == s.To {
			return nil, errors.New("Empty subset")
		}
		if s.To > df.nRows || s.To < 0 || s.From < 0 {
			return nil, errors.New("Subset out of range")
		}

		newDf.nRows = s.To - s.From
		for k, v := range df.Columns {
			col, err := newCol(v.colName, v.cells[s.From:s.To])
			if err != nil {
				return nil, err
			}
			col.recountNumChars()
			newDf.Columns[k] = *col
		}
	case []int:
		rowNums := subset.([]int)

		if len(rowNums) == 0 {
			return nil, errors.New("Empty subset")
		}

		// Check for errors
		for _, v := range rowNums {
			if v >= df.nRows {
				return nil, errors.New("Subset out of range")
			}
		}

		newDf.nRows = len(rowNums)
		for k, v := range df.Columns {
			cells := Cells{}

			for _, i := range rowNums {
				if i < 0 {
					cells = append(cells, v.empty)
				} else {
					cells = append(cells, v.cells[i])
				}
			}

			col, err := newCol(v.colName, cells)
			if err != nil {
				return nil, err
			}

			col.recountNumChars()
			newDf.Columns[k] = *col
		}
	default:
		return nil, errors.New("Unknown subsetting option")
	}

	return &newDf, nil
}

// Rbind combines the rows of two dataframes
func Rbind(a DataFrame, b DataFrame) (*DataFrame, error) {
	newDf := &DataFrame{
		nRows:     a.nRows + b.nRows,
		colIndexs: a.colIndexs,
		Columns:   map[string]column{},
	}

	for k, v := range a.colIndexs {
		idx, ok := b.colIndexs[k]
		if !ok || idx != v {
			return nil, errors.New("Mismatching column names")
		}

		if a.Columns[k].colType != b.Columns[k].colType {
			return nil, errors.New("Mismatching column types")
		}

		col := a.Columns[k].copy()
		var err error
		col, err = col.append(b.Columns[k].copy().cells...)
		if err != nil {
			return nil, err
		}
		newDf.Columns[k] = col
	}

	return newDf, nil
}

// Cbind combines the columns of two DataFrames
func Cbind(a DataFrame, b DataFrame) (*DataFrame, error) {
	// Check that the two DataFrames contains the same number of rows
	if a.nRows != b.nRows {
		return nil, errors.New("Different number of rows")
	}

	dfa := a.copy()
	dfb := b.copy()

	for k, v := range dfb.Columns {
		if _, ok := dfa.Columns[k]; ok {
			return nil, errors.New("Conflicting column names")
		}
		dfa.Columns[k] = v
	}

	for k, v := range dfb.colIndexs {
		dfa.colIndexs[k] = v + len(a.colIndexs)
	}

	return &dfa, nil
}

type b []byte

// uniqueRowsMap is a helper function that will get a map of unique or duplicated
// rows for a given DataFrame
func uniqueRowsMap(df DataFrame) map[string]u {
	uniqueRows := make(map[string]u)
	for i := 0; i < df.nRows; i++ {
		mdarr := []byte{}
		for _, v := range df.Columns {
			cs := v.cells[i].Checksum()
			mdarr = append(mdarr, cs[:]...)
		}
		str := string(mdarr)
		if a, ok := uniqueRows[str]; ok {
			a.unique = false
			a.appears = append(a.appears, i)
			uniqueRows[str] = a
		} else {
			uniqueRows[str] = u{true, []int{i}}
		}
	}

	return uniqueRows
}

// Unique will return all unique rows inside a DataFrame. The order of the rows
// will not be preserved.
func (df DataFrame) Unique() (*DataFrame, error) {
	uniqueRows := uniqueRowsMap(df)
	appears := []int{}
	for _, v := range uniqueRows {
		if v.unique {
			appears = append(appears, v.appears[0])
		}
	}

	return df.SubsetRows(appears)
}

// RemoveUnique will return all duplicated rows inside a DataFrame
func (df DataFrame) RemoveUnique() (*DataFrame, error) {
	uniqueRows := uniqueRowsMap(df)
	appears := []int{}
	for _, v := range uniqueRows {
		if !v.unique {
			appears = append(appears, v.appears...)
		}
	}

	return df.SubsetRows(appears)
}

// RemoveDuplicated will return all unique rows in a DataFrame and the first
// appearance of all duplicated rows. The order of the rows will not be
// preserved.
func (df DataFrame) RemoveDuplicated() (*DataFrame, error) {
	uniqueRows := uniqueRowsMap(df)
	appears := []int{}
	for _, v := range uniqueRows {
		appears = append(appears, v.appears[0])
	}

	return df.SubsetRows(appears)
}

// Duplicated will return the first appearance of the duplicated rows in
// a DataFrame. The order of the rows will not be preserved.
func (df DataFrame) Duplicated() (*DataFrame, error) {
	uniqueRows := uniqueRowsMap(df)
	appears := []int{}
	for _, v := range uniqueRows {
		if !v.unique {
			appears = append(appears, v.appears[0])
		}
	}

	return df.SubsetRows(appears)
}

// Implementing the Stringer interface for DataFrame
func (df DataFrame) String() (str string) {
	// TODO: We should truncate the maximum length of shown columns and scape newline
	// characters'
	addLeftPadding := func(s string, nchar int) string {
		if len(s) < nchar {
			return strings.Repeat(" ", nchar-len(s)) + s
		}
		return s
	}
	addRightPadding := func(s string, nchar int) string {
		if len(s) < nchar {
			return s + strings.Repeat(" ", nchar-len(s))
		}
		return s
	}

	nRowsPadding := len(fmt.Sprint(df.nRows))
	if df.NCols() != 0 {
		str += addLeftPadding("  ", nRowsPadding+2)
		for k, v := range df.Columns {
			str += addRightPadding(v.colName, df.Columns[k].numChars)
			str += "  "
		}
		str += "\n"
		str += "\n"
	}
	for i := 0; i < df.nRows; i++ {
		str += addLeftPadding(strconv.Itoa(i)+": ", nRowsPadding+2)
		for _, v := range df.Columns {
			elem := v.cells[i]
			str += addRightPadding(formatCell(elem), v.numChars)
			str += "  "
		}
		str += "\n"
	}

	return str
}

// formatCell returns the value of a given element in string format. In case of
// a nil pointer the value returned will be NA.
func formatCell(cell interface{}) string {
	if reflect.TypeOf(cell).Kind() == reflect.Ptr {
		if reflect.ValueOf(cell).IsNil() {
			return "NA"
		}
		val := reflect.Indirect(reflect.ValueOf(cell)).Interface()
		return fmt.Sprint(val)
	}
	return fmt.Sprint(cell)
}

// InnerJoin returns a DataFrame containing the inner join of two other DataFrames.
// This operation matches all rows that appear on both dataframes.
/*
func InnerJoin(a DataFrame, b DataFrame, keys ...string) (*DataFrame, error) {
	dfa := a.copy()
	dfb := b.copy()
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	for _, key := range keys {
		ca, oka := dfa.Columns[key]
		cb, okb := dfb.Columns[key]
		if !oka {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
		}
		if !okb {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on right DataFrame"))
		}
		// Check that the column types are the same between DataFrames
		if len(errorArr) == 0 {
			ta := ca.colType
			tb := cb.colType
			if ta != tb {
				errorArr = append(errorArr, fmt.Sprint("Different types for key\"", key, "\". Left:", ta, " Right:", tb))
			}
		}
	}
	if len(errorArr) != 0 {
		return nil, errors.New(strings.Join(errorArr, "\n"))
	}

	// Rename non key coumns with the same name on both DataFrames
	colnamesa := dfa.Names()
	colnamesb := dfb.Names()

	for k, v := range colnamesa {
		if idx, err := dfb.colIndex(v); err == nil {
			if !inStringSlice(v, keys) {
				colnamesa[k] = v + ".x"
				colnamesb[idx] = v + ".y"
			}
		}
	}
	dfa.SetNames(colnamesa)
	dfb.SetNames(colnamesb)

	// Get the column indexes of both columns for the given keys
	colIdxa := []int{}
	colIdxb := []int{}
	for _, key := range keys {
		ia, erra := dfa.colIndex(key)
		ib, errb := dfb.colIndex(key)
		if erra == nil && errb == nil {
			colIdxa = append(colIdxa, ia)
			colIdxb = append(colIdxb, ib)
		}
	}

	// Get the combined checksum for all keys in both DataFrames
	checksumsa := make([][]byte, dfa.nRows)
	checksumsb := make([][]byte, dfb.nRows)
	for _, i := range colIdxa {
		for k, v := range dfa.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsa[k] = append(checksumsa[k], b...)
		}
	}
	for _, i := range colIdxb {
		for k, v := range dfb.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsb[k] = append(checksumsb[k], b...)
		}
	}

	// Get the indexes of the rows we want to join
	dfaIndexes := []int{}
	dfbIndexes := []int{}
	for ka, ca := range checksumsa {
		for kb, cb := range checksumsb {
			if string(ca) == string(cb) {
				dfaIndexes = append(dfaIndexes, ka)
				dfbIndexes = append(dfbIndexes, kb)
			}
		}
	}

	// Get the names of the elements that are not keys on the right DataFrame
	nokeynamesb := []string{}
	for _, v := range dfb.Columns {
		if !inStringSlice(v.colName, keys) {
			nokeynamesb = append(nokeynamesb, v.colName)
		}
	}

	newdfa, _ := dfa.SubsetRows(dfaIndexes)
	newdfb, _ := dfb.Subset(nokeynamesb, dfbIndexes)
	return Cbind(*newdfa, *newdfb)
}

// CrossJoin returns a DataFrame containing the cartesian product of the rows on
// both DataFrames.
func CrossJoin(a DataFrame, b DataFrame) (*DataFrame, error) {
	dfa := a.copy()
	dfb := b.copy()
	colnamesa := dfa.Names()
	colnamesb := dfb.Names()

	for k, v := range dfa.Columns {
		if idx, err := dfb.colIndex(v.colName); err == nil {
			colnamesa[k] = v.colName + ".x"
			colnamesb[idx] = v.colName + ".y"
		}
	}
	dfa.SetNames(colnamesa)
	dfb.SetNames(colnamesb)

	// Get the indexes of the rows we want to join
	dfaIndexes := []int{}
	dfbIndexes := []int{}
	for i := 0; i < dfa.nRows; i++ {
		for j := 0; j < dfb.nRows; j++ {
			dfaIndexes = append(dfaIndexes, i)
			dfbIndexes = append(dfbIndexes, j)
		}
	}

	newdfa, _ := dfa.SubsetRows(dfaIndexes)
	newdfb, _ := dfb.SubsetRows(dfbIndexes)
	return Cbind(*newdfa, *newdfb)
}

// LeftJoin returns a DataFrame containing the left join of two other DataFrames.
// This operation matches all rows that appear on the left DataFrame and matches
// it with the existing ones on the right one, filling the missing rows on the
// right with an empty value.
func LeftJoin(a DataFrame, b DataFrame, keys ...string) (*DataFrame, error) {
	dfa := a.copy()
	dfb := b.copy()
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	for _, key := range keys {
		ia, erra := dfa.colIndex(key)
		ib, errb := dfb.colIndex(key)
		if erra != nil {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
		}
		if errb != nil {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on right DataFrame"))
		}
		// Check that the column types are the same between DataFrames
		if len(errorArr) == 0 {
			ta := dfa.Columns[ia].colType
			tb := dfb.Columns[ib].colType
			if ta != tb {
				errorArr = append(errorArr, fmt.Sprint("Different types for key\"", key, "\". Left:", ta, " Right:", tb))
			}
		}
	}
	if len(errorArr) != 0 {
		return nil, errors.New(strings.Join(errorArr, "\n"))
	}

	// Rename non key coumns with the same name on both DataFrames
	colnamesa := dfa.Names()
	colnamesb := dfb.Names()

	for k, v := range colnamesa {
		if !inStringSlice(v, keys) {
			if idx, err := dfb.colIndex(v); err == nil {
				colnamesa[k] = v + ".x"
				colnamesb[idx] = v + ".y"
			}
		}
	}
	dfa.SetNames(colnamesa)
	dfb.SetNames(colnamesb)

	// Get the column indexes of both columns for the given keys
	colIdxa := []int{}
	colIdxb := []int{}
	for _, key := range keys {
		ia, erra := dfa.colIndex(key)
		ib, errb := dfb.colIndex(key)
		if erra == nil && errb == nil {
			colIdxa = append(colIdxa, ia)
			colIdxb = append(colIdxb, ib)
		}
	}

	// Get the combined checksum for all keys in both DataFrames
	checksumsa := make([][]byte, dfa.nRows)
	checksumsb := make([][]byte, dfb.nRows)
	for _, i := range colIdxa {
		for k, v := range dfa.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsa[k] = append(checksumsa[k], b...)
		}
	}
	for _, i := range colIdxb {
		for k, v := range dfb.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsb[k] = append(checksumsb[k], b...)
		}
	}

	// Get the indexes of the rows we want to join
	dfaIndexes := []int{}
	dfbIndexes := []int{}
	for ka, ca := range checksumsa {
		found := false
		for kb, cb := range checksumsb {
			if string(ca) == string(cb) {
				dfaIndexes = append(dfaIndexes, ka)
				dfbIndexes = append(dfbIndexes, kb)
				found = true
			}
		}
		if !found {
			dfaIndexes = append(dfaIndexes, ka)
			dfbIndexes = append(dfbIndexes, -1)

		}
	}

	// Get the names of the elements that are not keys on the right DataFrame
	nokeynamesb := []string{}
	for _, v := range dfb.Columns {
		if !inStringSlice(v.colName, keys) {
			nokeynamesb = append(nokeynamesb, v.colName)
		}
	}

	newdfa, _ := dfa.SubsetRows(dfaIndexes)
	newdfb, _ := dfb.Subset(nokeynamesb, dfbIndexes)
	return Cbind(*newdfa, *newdfb)
}

// RightJoin returns a DataFrame containing the right join of two other DataFrames.
// This operation matches all rows that appear on the right DataFrame and matches
// it with the existing ones on the left one, filling the missing rows on the
// left with an empty value.
func RightJoin(b DataFrame, a DataFrame, keys ...string) (*DataFrame, error) {
	dfa := a.copy()
	dfb := b.copy()
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	for _, key := range keys {
		ia, erra := dfa.colIndex(key)
		ib, errb := dfb.colIndex(key)
		if erra != nil {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on left DataFrame"))
		}
		if errb != nil {
			errorArr = append(errorArr, fmt.Sprint("Can't find key \"", key, "\" on right DataFrame"))
		}
		// Check that the column types are the same between DataFrames
		if len(errorArr) == 0 {
			ta := dfa.Columns[ia].colType
			tb := dfb.Columns[ib].colType
			if ta != tb {
				errorArr = append(errorArr, fmt.Sprint("Different types for key\"", key, "\". Left:", ta, " Right:", tb))
			}
		}
	}
	if len(errorArr) != 0 {
		return nil, errors.New(strings.Join(errorArr, "\n"))
	}

	// Rename non key coumns with the same name on both DataFrames
	colnamesa := dfa.Names()
	colnamesb := dfb.Names()

	for k, v := range colnamesa {
		if !inStringSlice(v, keys) {
			if idx, err := dfb.colIndex(v); err == nil {
				colnamesa[k] = v + ".y"
				colnamesb[idx] = v + ".x"
			}
		}
	}
	dfa.SetNames(colnamesa)
	dfb.SetNames(colnamesb)

	// Get the column indexes of both columns for the given keys
	colIdxa := []int{}
	colIdxb := []int{}
	for _, key := range keys {
		ia, erra := dfa.colIndex(key)
		ib, errb := dfb.colIndex(key)
		if erra == nil && errb == nil {
			colIdxa = append(colIdxa, ia)
			colIdxb = append(colIdxb, ib)
		}
	}

	// Get the combined checksum for all keys in both DataFrames
	checksumsa := make([][]byte, dfa.nRows)
	checksumsb := make([][]byte, dfb.nRows)
	for _, i := range colIdxa {
		for k, v := range dfa.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsa[k] = append(checksumsa[k], b...)
		}
	}
	for _, i := range colIdxb {
		for k, v := range dfb.Columns[i].cells {
			b := []byte{}
			cs := v.Checksum()
			b = append(b, cs[:]...)
			checksumsb[k] = append(checksumsb[k], b...)
		}
	}

	// Get the indexes of the rows we want to join
	dfaIndexes := []int{}
	dfbIndexes := []int{}
	for ka, ca := range checksumsa {
		found := false
		for kb, cb := range checksumsb {
			if string(ca) == string(cb) {
				dfaIndexes = append(dfaIndexes, ka)
				dfbIndexes = append(dfbIndexes, kb)
				found = true
			}
		}
		if !found {
			dfaIndexes = append(dfaIndexes, ka)
			dfbIndexes = append(dfbIndexes, -1)

		}
	}

	// Get the names of the elements that are not keys on the right DataFrame
	nokeynamesb := []string{}
	for _, v := range dfb.Columns {
		if !inStringSlice(v.colName, keys) {
			nokeynamesb = append(nokeynamesb, v.colName)
		}
	}

	newdfa, _ := dfa.SubsetRows(dfaIndexes)
	newdfb, _ := dfb.Subset(nokeynamesb, dfbIndexes)
	return Cbind(*newdfa, *newdfb)
}
*/

func (d DataFrame) GetCell(colname string, row int) (Cell, string, error) {
	col, ok := d.Columns[colname]
	if !ok {
		return nil, "", errors.New(colname + " not exsits")
	}

	if row >= d.NRows() {
		return nil, "", errors.New("row out of range: " + fmt.Sprint(row))
	}

	return col.cells[row], col.colType, nil
}

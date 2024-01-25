package orm

import (
	"fmt"
	"strconv"
	"strings"
)

type WhereClause struct {
	Query       string              `json:"query"`
	Args        []interface{}       `json:"args"`
	Preload     []string            `json:"preload"`
	Limit       int                 `json:"limit"`
	Offset      int                 `json:"offset"`
	OrderByASC  string              `json:"orderByASC"`
	OrderByDESC string              `json:"orderByDESC"`
	Page        int                 `json:"page"`
	PageSize    int                 `json:"pageSize"`
	Aggregates  map[string][]string `json:"aggregates"`
}

func (w *WhereClause) AddPreload(preload string) *WhereClause {
	w.Preload = append(w.Preload, preload)
	return w
}

/*
NewWhereClause

w := NewWhereClause().Where("status = ?", "active")

	.And("created_at > ?", someDate)
	.Like("title", "%example%")
	.IsNot("archived", true)

This creates a WhereClause with the query "status = ? AND created_at > ? AND title LIKE ? AND archived IS NOT ?",
and the corresponding arguments ["active", someDate, "%example%", true].
*/
func NewWhereClause() *WhereClause {
	return &WhereClause{}
}

func NewWhere(query string, args []interface{}, limit, offset int) *WhereClause {
	return &WhereClause{
		Query:  query,
		Args:   args,
		Limit:  limit,
		Offset: offset,
	}
}

func (w *WhereClause) Where(query string, args ...interface{}) *WhereClause {
	w.Query = query
	w.Args = args
	return w
}

func (w *WhereClause) AddAnd() *WhereClause {
	if w.Query != "" {
		w.Query += " AND "
	}
	return w
}

func (w *WhereClause) AddOr() *WhereClause {
	if w.Query != "" {
		w.Query += " OR "
	}
	return w
}

func (w *WhereClause) And(query string, args ...interface{}) *WhereClause {
	w.Query += " AND " + query // Add a space before "AND"
	w.Args = append(w.Args, args...)
	return w
}

func (w *WhereClause) Or(query string, args ...interface{}) *WhereClause {
	w.Query += " OR " + query // Add a space before "OR"
	w.Args = append(w.Args, args...)
	return w
}

func (w *WhereClause) LimitClause(limit int) *WhereClause {
	w.Limit = limit
	return w
}

func (w *WhereClause) OffsetClause(offset int) *WhereClause {
	w.Offset = offset
	return w
}

func (w *WhereClause) Like(field, value string) *WhereClause {
	w.Query += fmt.Sprintf("%s LIKE ?", field)
	w.Args = append(w.Args, value)
	return w
}

func (w *WhereClause) Is(field string, value interface{}) *WhereClause {
	w.Query += fmt.Sprintf("%s IS ?", field)
	w.Args = append(w.Args, value)
	return w
}

func (w *WhereClause) IsNot(field string, value interface{}) *WhereClause {
	w.Query += fmt.Sprintf("%s IS NOT ?", field)
	w.Args = append(w.Args, value)
	return w
}

func (w *WhereClause) GreaterThan(field string, value interface{}) *WhereClause {
	w.Query += fmt.Sprintf("%s > ?", field)
	w.Args = append(w.Args, value)
	return w
}

func (w *WhereClause) LessThanOrEqual(field string, value interface{}) *WhereClause {
	w.Query += fmt.Sprintf("%s <= ?", field)
	w.Args = append(w.Args, value)
	return w
}

func (w *WhereClause) GreaterThanOrEqual(field string, value interface{}) *WhereClause {
	w.Query += fmt.Sprintf("%s >= ?", field)
	w.Args = append(w.Args, value)
	return w
}

func (w *WhereClause) NotEqual(field string, value interface{}) *WhereClause {
	w.Query += fmt.Sprintf("%s != ?", field)
	w.Args = append(w.Args, value)
	return w
}

func (w *WhereClause) Equal(field string, value interface{}) *WhereClause {
	w.Query += fmt.Sprintf("%s = ?", field)
	w.Args = append(w.Args, value)
	return w
}

// DateRange
// This creates a WhereClause with the query "status = ? AND created_at >= ? AND created_at <= ?",
// and the corresponding arguments ["active", "2021-01-01", "2021-12-31"].
func (w *WhereClause) DateRange(field string, startDate, endDate interface{}) *WhereClause {
	w.Query += fmt.Sprintf("%s >= ? AND %s <= ?", field, field)
	w.Args = append(w.Args, startDate, endDate)
	return w
}

func (w *WhereClause) LengthGreaterThan(field string, length int) *WhereClause {
	w.Query += fmt.Sprintf("LENGTH(%s) > ?", field)
	w.Args = append(w.Args, length)
	return w
}

func BuildWhereClause(queryString string) (*WhereClause, error) {
	whereClause := &WhereClause{
		Aggregates: make(map[string][]string),
	}

	// Split the query string at '&' to get AND-separated conditions
	andConditions := strings.Split(queryString, "&")

	for _, andCond := range andConditions {
		if strings.HasPrefix(andCond, "agg__") {
			aggregateParts := strings.SplitN(andCond, "=", 2)
			if len(aggregateParts) == 2 {
				aggFunction, fields := aggregateParts[0], aggregateParts[1]
				aggFunction = strings.TrimPrefix(aggFunction, "agg__")
				whereClause.Aggregates[aggFunction] = strings.Split(fields, "|")
				continue // Skip adding aggregates to the main query
			}
		}

		if andCond == "" {
			continue
		}
		if strings.HasPrefix(andCond, "useData") { // ignore this
			continue
		}
		if strings.HasPrefix(andCond, "useCache") { // ignore this
			continue
		}

		// Process preloading directives first
		if strings.HasPrefix(andCond, "with_") {
			preloadValues := strings.TrimPrefix(andCond, "with_")
			for _, preloadDirective := range strings.Split(preloadValues, "|") {
				whereClause.Preload = append(whereClause.Preload, preloadDirective)
			}
			continue
		}

		// Handle preloading and pagination
		if parts := strings.SplitN(andCond, "=", 2); len(parts) == 2 {
			key, value := parts[0], parts[1]

			switch {
			case key == "orderByASC": // Handle orderBy differently
				whereClause.OrderByASC = value
				continue
			case key == "orderByDESC": // Handle orderBy differently
				whereClause.OrderByDESC = value
				continue
			case key == "limit", key == "offset", key == "page", key == "pageSize":
				if err := setPaginationField(whereClause, key, value); err != nil {
					return nil, err
				}
				continue
			}
		}

		// Process OR conditions within each AND condition
		orConditions := strings.Split(andCond, "|")
		orConditionParts := []string{}

		for _, orCond := range orConditions {
			cond, args, err := processCondition(orCond)
			if err != nil {
				return nil, err
			}
			if cond != "" {
				orConditionParts = append(orConditionParts, cond)
				whereClause.Args = append(whereClause.Args, args...)
			}
		}

		if len(orConditionParts) > 0 {
			// Join the OR conditions
			groupQuery := "(" + strings.Join(orConditionParts, " OR ") + ")"
			if whereClause.Query != "" {
				whereClause.Query += " AND "
			}
			whereClause.Query += groupQuery
		}
	}

	return whereClause, nil
}

func setPaginationField(wc *WhereClause, key, value string) error {
	val, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	switch key {
	case "limit":
		wc.Limit = val
	case "offset":
		wc.Offset = val
	case "page":
		wc.Page = val
	case "pageSize":
		wc.PageSize = val
	}
	return nil
}

func processCondition(condition string) (string, []interface{}, error) {
	parts := strings.SplitN(condition, "=", 2)
	if len(parts) != 2 {
		return "", nil, nil // Skip if format is not 'key=value'
	}

	key, value := parts[0], parts[1]
	fieldParts := strings.SplitN(key, "__", 2)
	field := fieldParts[0]
	operator := "=" // Default operator

	if len(fieldParts) == 2 {
		switch fieldParts[1] {
		case "gt":
			operator = ">"
		case "gte":
			operator = ">="
		case "lt":
			operator = "<"
		case "lte":
			operator = "<="
		case "ne":
			operator = "!="
		case "not":
			operator = "NOT"

		}
	}

	condition = field + " " + operator + " ?"
	return condition, []interface{}{value}, nil
}

// Example usage:
// whereClause, err := BuildWhereClause("firstName__ne=John&lastName=Smith|age__gt=30&with_team|with_friends&limit=10&offset=20&page=2&pageSize=10")
// if err != nil {
//     // handle error
// }

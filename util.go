package gomocket

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
)

// Regexp to replace multiple spaces with single space
var whitespaces = regexp.MustCompile(`\s+`)

func normalize(origin string) string {
	s := strings.TrimSpace(origin)
	s = whitespaces.ReplaceAllString(s, " ")
	return s
}

func completeStatement(prepareStatment string, args []driver.NamedValue) (query string) {
	if !strings.Contains(prepareStatment, "?") || len(args) == 0 {
		return prepareStatment
	}
	for _, arg := range args {
		var value string
		switch arg.Value.(type) {
		case int, int32, int64, uint, uint32, uint64:
			value = fmt.Sprintf("%d", arg.Value)
		case string:
			value = fmt.Sprintf("%s", arg.Value)
		case []byte:
			value = fmt.Sprintf("%s", arg.Value)
		default:
			value = fmt.Sprintf("%v", arg.Value)
		}
		prepareStatment = strings.Replace(prepareStatment, "?", value, 1)
	}
	return prepareStatment
}

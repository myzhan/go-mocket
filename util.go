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
		value := fmt.Sprintf("%v", arg.Value)
		prepareStatment = strings.Replace(prepareStatment, "?", value, 1)
	}
	return prepareStatment
}

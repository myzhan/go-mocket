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
	// Replace all "?" to "%v" and replace them with the values after
	for _, arg := range args {
		prepareStatment = strings.Replace(prepareStatment, "?", "%v", 1)
		prepareStatment = fmt.Sprintf(prepareStatment, arg.Value)
	}
	return prepareStatment
}

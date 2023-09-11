package gomocket

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"
)

const (
	// DriverName is the name of the fake driver
	DriverName = "MOCK_FAKE_DRIVER"
)

const (
	// Predefined match priority
	GLOBAL = iota
	TESTSUITE
	TESTCASE
)

// Catcher is global instance of Catcher used for attaching all mocks to connection
var Catcher *MockCatcher

// MockCatcher is global entity to save all mocks aka FakeResponses
type MockCatcher struct {
	Mocks                []*FakeResponse // Slice of all mocks
	ReceivedQueries      map[string]int  // All received queries
	NoMatchingQueries    map[string]int  // All queries that didn't match any mock
	Logging              bool            // Do we need to log what we catching?
	PanicOnEmptyResponse bool            // If not response matches - do we need to panic?
	mu                   sync.Mutex
}

func (mc *MockCatcher) SetLogging(l bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.Logging = l
}

// Register safely register FakeDriver
func (mc *MockCatcher) Register() {
	driversList := sql.Drivers()
	for _, name := range driversList {
		if name == DriverName {
			return
		}
	}
	sql.Register(DriverName, &FakeDriver{})
}

// Attach several mocks to MockCather. Could be useful to attach mocks from some factories of mocks
func (mc *MockCatcher) Attach(fr []*FakeResponse) {
	for _, r := range fr {
		r.Pattern = normalize(r.Pattern)
		mc.Mocks = append(mc.Mocks, r)
	}
}

// FindResponse finds suitable response by provided
func (mc *MockCatcher) FindResponse(query string, args []driver.NamedValue) *FakeResponse {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	query = normalize(query)

	query_with_args := completeStatement(query, args)
	if times, ok := mc.ReceivedQueries[query_with_args]; ok {
		mc.ReceivedQueries[query_with_args] = times + 1
	} else {
		mc.ReceivedQueries[query_with_args] = 1
	}

	sort.SliceStable(mc.Mocks, func(i, j int) bool {
		if mc.Mocks[i].MatchPriority != mc.Mocks[j].MatchPriority {
			return mc.Mocks[i].MatchPriority > mc.Mocks[j].MatchPriority
		} else {
			return len(mc.Mocks[i].Pattern) > len(mc.Mocks[j].Pattern)
		}
	})

	for _, resp := range mc.Mocks {
		if resp.IsMatch(query, args) {
			if mc.Logging {
				log.Printf("mock_catcher: [MATCHED QUERY]: %s matches mock {pattern: %s, args: %v}", query, resp.Pattern, resp.Args)
			}
			resp.MarkAsTriggered()
			resp.TriggeredTimes++
			return resp
		}
	}

	if times, ok := mc.NoMatchingQueries[query_with_args]; ok {
		mc.NoMatchingQueries[query_with_args] = times + 1
	} else {
		mc.NoMatchingQueries[query_with_args] = 1
	}

	if mc.Logging {
		log.Printf("mock_catcher: [NO MATCHED QUERY]: %s doesn't match anything", query)
	}

	if mc.PanicOnEmptyResponse {
		panic(fmt.Sprintf("No responses matches query %s ", query))
	}

	// Let's have always dummy version of response
	return &FakeResponse{
		Response:   make([]map[string]interface{}, 0),
		Exceptions: &Exceptions{},
	}
}

// NewMock creates new FakeResponse and return for chains of attachments
func (mc *MockCatcher) NewMock() *FakeResponse {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	fr := &FakeResponse{Exceptions: &Exceptions{}, Response: make([]map[string]interface{}, 0)}
	mc.Mocks = append(mc.Mocks, fr)
	return fr
}

// ExpectationOfTriggeredTimesIsMeet checks the triggered times are as expected
func (mc *MockCatcher) ExpectationOfTriggeredTimesIsMeet() (bool, []string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	msgs := []string{}
	for _, resp := range mc.Mocks {
		if resp.ExpectedTriggeredTimes == 0 {
			continue
		}
		if resp.ExpectedTriggeredTimes != resp.TriggeredTimes {
			msgs = append(msgs, fmt.Sprintf("We are expecting %s to be triggered %d times, but got %d", resp.Pattern, resp.ExpectedTriggeredTimes, resp.TriggeredTimes))
		}
	}

	return len(msgs) == 0, msgs
}

// FindReceivedQuery checks how many times the query has been sent
func (mc *MockCatcher) FindReceivedQuery(query string) (ok bool, times int) {
	if times, ok = mc.ReceivedQueries[query]; ok {
		return ok, times
	} else {
		return ok, 0
	}
}

// FindNoMatchingQuery checks how many times the query has not been matched
func (mc *MockCatcher) FindNoMatchingQuery(query string) (ok bool, times int) {
	if times, ok = mc.NoMatchingQueries[query]; ok {
		return ok, times
	} else {
		return ok, 0
	}
}

// Reset removes all Mocks to start process again
func (mc *MockCatcher) Reset() *MockCatcher {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.Mocks = make([]*FakeResponse, 0)
	mc.ReceivedQueries = make(map[string]int)
	mc.NoMatchingQueries = make(map[string]int)
	return mc
}

// Exceptions represents	 possible exceptions during query executions
type Exceptions struct {
	HookQueryBadConnection func() bool
	HookExecBadConnection  func() bool
}

// FakeResponse represents mock of response with holding all required values to return mocked response
type FakeResponse struct {
	Pattern                string                            // SQL query pattern to match with
	MatchPriority          int                               // MatchPriority defines priority of matching, higher value will be picked up first
	Strict                 bool                              // Strict SQL query pattern comparison or by strings.Contains()
	Args                   []interface{}                     // List args to be matched with
	Response               []map[string]interface{}          // Array of rows to be parsed as result
	Once                   bool                              // To trigger only once
	Triggered              bool                              // If it was triggered at least once
	ExpectedTriggeredTimes uint32                            // How many times we are expecting to be triggerd
	TriggeredTimes         uint32                            // How many times that has been triggerd
	Callback               func(string, []driver.NamedValue) // Callback to execute when response triggered
	RowsAffected           int64                             // Defines affected rows count
	LastInsertID           int64                             // ID to be returned for INSERT queries
	Error                  error                             // Any type of error which could happen dur
	mu                     sync.Mutex                        // Used to lock concurrent access to variables
	*Exceptions
}

// isArgsMatch returns true either when nothing to compare or deep equal check passed
func (fr *FakeResponse) isArgsMatch(args []driver.NamedValue) bool {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	arguments := make([]interface{}, len(args))
	if len(args) > 0 {
		for index, arg := range args {
			arguments[index] = arg.Value
		}
	}
	return fr.Args == nil || reflect.DeepEqual(fr.Args, arguments)
}

// isQueryMatch returns true if searched query is matched FakeResponse Pattern
func (fr *FakeResponse) isQueryMatch(query string) bool {
	fr.mu.Lock()
	defer fr.mu.Unlock()

	if fr.Pattern == "" {
		return true
	}

	if fr.Strict == true && query == fr.Pattern {
		return true
	}

	if fr.Strict == false && strings.Contains(query, fr.Pattern) {
		return true
	}

	return false
}

// IsMatch checks if both query and args matcher's return true and if this is Once mock
func (fr *FakeResponse) IsMatch(query string, args []driver.NamedValue) bool {
	fr.mu.Lock()
	if fr.Once && fr.Triggered {
		fr.mu.Unlock()
		return false
	}
	fr.mu.Unlock()
	return fr.isQueryMatch(query) && fr.isArgsMatch(args)
}

// MarkAsTriggered marks response as executed. For one time catches it will not make this possible to execute anymore
func (fr *FakeResponse) MarkAsTriggered() {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	fr.Triggered = true
}

// WithQuery adds SQL query pattern to match for
func (fr *FakeResponse) WithQuery(query string) *FakeResponse {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	fr.Pattern = normalize(query)
	return fr
}

// WithQuery adds SQL query pattern to match for
func (fr *FakeResponse) StrictMatch() *FakeResponse {
	fr.Strict = true
	return fr
}

// WithArgs attaches Args check for prepared statements
func (fr *FakeResponse) WithArgs(vars ...interface{}) *FakeResponse {
	if len(vars) > 0 {
		fr.Args = make([]interface{}, len(vars))
		for index, v := range vars {
			fr.Args[index] = v
		}
	}
	return fr
}

// WithReply adds to chain and assign some parts of response
func (fr *FakeResponse) WithReply(response []map[string]interface{}) *FakeResponse {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	fr.Response = response
	return fr
}

// OneTime sets current mock to be triggered only once
func (fr *FakeResponse) OneTime() *FakeResponse {
	fr.Once = true
	return fr
}

// WithExecException says that if mock attached to non-SELECT query we need to trigger error there
func (fr *FakeResponse) WithExecException() *FakeResponse {
	fr.Exceptions.HookExecBadConnection = func() bool {
		return true
	}
	return fr
}

// WithQueryException adds to SELECT mocks triggering of error
func (fr *FakeResponse) WithQueryException() *FakeResponse {
	fr.Exceptions.HookQueryBadConnection = func() bool {
		return true
	}
	return fr
}

// WithCallback adds callback to be executed during matching
func (fr *FakeResponse) WithCallback(f func(string, []driver.NamedValue)) *FakeResponse {
	fr.Callback = f
	return fr
}

// WithRowsNum specifies how many records to consider as affected
func (fr *FakeResponse) WithRowsNum(num int64) *FakeResponse {
	fr.RowsAffected = num
	return fr
}

// WithID sets ID to be considered as insert ID for INSERT statements
func (fr *FakeResponse) WithID(id int64) *FakeResponse {
	fr.LastInsertID = id
	return fr
}

// WithError sets Error to FakeResponse struct to have it available on any statements executed
// example: WithError(sql.ErrNoRows)
func (fr *FakeResponse) WithError(err error) *FakeResponse {
	fr.Error = err
	return fr
}

// WithExpectedTriggerTimes sets expected trigger times
// example: WithExpectedTriggerTimes(uint32(2))
func (fr *FakeResponse) WithExpectedTriggerTimes(expected uint32) *FakeResponse {
	fr.ExpectedTriggeredTimes = expected
	return fr
}

// WithMatchPriority sets priority
func (fr *FakeResponse) WithMatchPriority(priority int) *FakeResponse {
	fr.MatchPriority = priority
	return fr
}

func init() {
	Catcher = &MockCatcher{
		ReceivedQueries:   make(map[string]int),
		NoMatchingQueries: make(map[string]int),
	}
}

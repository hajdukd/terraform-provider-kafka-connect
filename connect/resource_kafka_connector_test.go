package connect

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	r "github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	kc "github.com/ricardo-ch/go-kafka-connect/v3/lib/connectors"
)

func TestAccConnectorConfigUpdate(t *testing.T) {
	r.Test(t, r.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testProviders,
		Steps: []r.TestStep{
			{
				Config: testResourceConnector_initialConfig,
				Check:  testResourceConnector_initialCheck,
			},
			{
				Config:            testResourceConnector_initialConfig,
				ResourceName:      "kafka-connect_connector.test",
				ImportStateVerify: true,
				ImportState:       true,
			},
			{
				Config: testResourceConnector_updateConfig,
				Check:  testResourceConnector_updateCheck,
			},
		},
	})
}

// TestAccConnectorWithTimeouts tests that custom timeouts are properly accepted
func TestAccConnectorWithTimeouts(t *testing.T) {
	r.Test(t, r.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testProviders,
		Steps: []r.TestStep{
			{
				Config: testResourceConnector_withTimeouts,
				Check: r.ComposeTestCheckFunc(
					r.TestCheckResourceAttr("kafka-connect_connector.test_timeouts", "name", "test-with-timeouts"),
					r.TestCheckResourceAttr("kafka-connect_connector.test_timeouts", "config.tasks.max", "1"),
					// Verify the connector was created (if it has an ID, it was created successfully)
					r.TestCheckResourceAttrSet("kafka-connect_connector.test_timeouts", "id"),
				),
			},
		},
	})
}

func testResourceConnector_initialCheck(s *terraform.State) error {
	resourceState := s.Modules[0].Resources["kafka-connect_connector.test"]
	if resourceState == nil {
		return fmt.Errorf("resource not found in state")
	}

	instanceState := resourceState.Primary
	if instanceState == nil {
		return fmt.Errorf("resource has no primary instance")
	}

	name := instanceState.ID

	if name != instanceState.Attributes["name"] {
		return fmt.Errorf("id doesn't match name")
	}

	client := testProvider.Meta().(kc.HighLevelClient)

	c, err := client.GetConnector(kc.ConnectorRequest{Name: "sqlite-sink"})
	if err != nil {
		return err
	}

	tasksMax := c.Config["tasks.max"]
	expected := "2"
	if tasksMax != expected {
		return fmt.Errorf("tasks.max should be %s, got %s connector not updated. \n %v", expected, tasksMax, c.Config)
	}

	return nil
}

func testResourceConnector_updateCheck(s *terraform.State) error {
	client := testProvider.Meta().(kc.HighLevelClient)

	c, err := client.GetConnector(kc.ConnectorRequest{Name: "sqlite-sink"})
	if err != nil {
		return err
	}

	tasksMax := c.Config["tasks.max"]
	expected := "1"
	if tasksMax != expected {
		return fmt.Errorf("tasks.max should be %s, got %s connector not updated. \n %v", expected, tasksMax, c.Config)
	}

	return nil
}

const testResourceConnector_initialConfig = `
resource "kafka-connect_connector" "test" {
  name = "sqlite-sink"

  config = {
		"name" = "sqlite-sink"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "2"
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }
}
`

const testResourceConnector_updateConfig = `
resource "kafka-connect_connector" "test" {
  name = "sqlite-sink"

  config = {
		"name" = "sqlite-sink"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "1"
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }
}
`

// Test configuration with custom timeouts
const testResourceConnector_withTimeouts = `
resource "kafka-connect_connector" "test_timeouts" {
  name = "test-with-timeouts"

  config = {
    "name"            = "test-with-timeouts"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "1"
    "topics"          = "test-topic"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }

  timeouts {
    create = "10m"
    update = "8m"
    delete = "3m"
  }
}
`

func TestStripInternalConfig(t *testing.T) {
	input := map[string]interface{}{
		"name":                      "sqlite-sink",
		"connector.class":           "io.confluent.connect.jdbc.JdbcSinkConnector",
		"tasks.max":                 "2",
		"__internal.config.version": "1",
		"__internal.foo":            "bar",
		"__internal":                "bare",
	}

	got := stripInternalConfig(input)

	if _, ok := got["__internal.config.version"]; ok {
		t.Errorf("__internal.config.version should be dropped, got %v", got["__internal.config.version"])
	}
	if _, ok := got["__internal.foo"]; ok {
		t.Errorf("__internal.foo should be dropped, got %v", got["__internal.foo"])
	}
	if _, ok := got["__internal"]; ok {
		t.Errorf("__internal should be dropped, got %v", got["__internal"])
	}
	if got["name"] != "sqlite-sink" {
		t.Errorf("name should be kept, got %v", got["name"])
	}
	if got["connector.class"] != "io.confluent.connect.jdbc.JdbcSinkConnector" {
		t.Errorf("connector.class should be kept, got %v", got["connector.class"])
	}
	if got["tasks.max"] != "2" {
		t.Errorf("tasks.max should be kept, got %v", got["tasks.max"])
	}
	if len(got) != 3 {
		t.Errorf("expected 3 remaining keys, got %d (%v)", len(got), got)
	}

	if _, ok := input["__internal.config.version"]; !ok {
		t.Errorf("input map was mutated: __internal.config.version missing")
	}
	if _, ok := input["__internal.foo"]; !ok {
		t.Errorf("input map was mutated: __internal.foo missing")
	}
	if len(input) != 6 {
		t.Errorf("input map was mutated: expected 6 keys, got %d", len(input))
	}
}

func TestIsMaskedValue(t *testing.T) {
	cases := []struct {
		name string
		v    interface{}
		want bool
	}{
		{name: "bullets", v: "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022", want: true},
		{name: "single bullet", v: "\u2022", want: true},
		{name: "stars", v: "********", want: true},
		{name: "single star", v: "*", want: true},
		{name: "hidden", v: "[hidden]", want: true},
		{name: "empty", v: "", want: false},
		{name: "plaintext", v: "s3cret", want: false},
		{name: "mixed masks", v: "\u2022*", want: false},
		{name: "hidden with suffix", v: "[hidden] ", want: false},
		{name: "hidden case", v: "[HIDDEN]", want: false},
		{name: "number", v: 1, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMaskedValue(tc.v); got != tc.want {
				t.Errorf("isMaskedValue(%#v) = %v, want %v", tc.v, got, tc.want)
			}
		})
	}
}

func TestConfigMismatches(t *testing.T) {
	t.Run("skips __internal keys", func(t *testing.T) {
		desired := map[string]interface{}{
			"name":                      "sqlite-sink",
			"__internal.config.version": "1",
			"__internal":                "local",
		}
		deployed := map[string]interface{}{
			"name":                      "sqlite-sink",
			"__internal.config.version": "99",
		}
		got := configMismatches(desired, deployed, nil)
		if len(got) != 0 {
			t.Errorf("expected no mismatches, got %v", got)
		}
	})

	t.Run("skips sensitive keys", func(t *testing.T) {
		desired := map[string]interface{}{
			"name":              "sqlite-sink",
			"database.password": "s3cret",
		}
		deployed := map[string]interface{}{
			"name":              "sqlite-sink",
			"database.password": "different-and-not-masked",
		}
		ignore := map[string]interface{}{
			"database.password": "s3cret",
		}
		got := configMismatches(desired, deployed, ignore)
		if len(got) != 0 {
			t.Errorf("expected sensitive key to be skipped, got %v", got)
		}
	})

	t.Run("skips masked deployed values", func(t *testing.T) {
		desired := map[string]interface{}{
			"name":              "sqlite-sink",
			"database.password": "s3cret",
			"api.key":           "real-key",
		}
		deployed := map[string]interface{}{
			"name":              "sqlite-sink",
			"database.password": "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022",
			"api.key":           "[hidden]",
		}
		got := configMismatches(desired, deployed, nil)
		if len(got) != 0 {
			t.Errorf("expected masked values to be skipped, got %v", got)
		}
	})

	t.Run("detects a real diff", func(t *testing.T) {
		desired := map[string]interface{}{
			"name":      "sqlite-sink",
			"tasks.max": "2",
			"topics":    "orders",
			"missing":   "x",
		}
		deployed := map[string]interface{}{
			"name":      "sqlite-sink",
			"tasks.max": "1",
			"topics":    "orders",
		}
		got := configMismatches(desired, deployed, nil)
		want := []string{"missing", "tasks.max"}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("ignores deployed-only keys", func(t *testing.T) {
		desired := map[string]interface{}{
			"name":      "sqlite-sink",
			"tasks.max": "1",
		}
		deployed := map[string]interface{}{
			"name":                      "sqlite-sink",
			"tasks.max":                 "1",
			"__internal.config.version": "7",
			"server.only":               "ignore-me",
		}
		got := configMismatches(desired, deployed, nil)
		if len(got) != 0 {
			t.Errorf("expected deployed-only keys to be ignored, got %v", got)
		}
	})
}

type fakeHighLevelClient struct {
	kc.HighLevelClient
	getConfig func(kc.ConnectorRequest) (kc.GetConnectorConfigResponse, error)
}

func (f *fakeHighLevelClient) GetConnectorConfig(req kc.ConnectorRequest) (kc.GetConnectorConfigResponse, error) {
	return f.getConfig(req)
}

func TestWaitForConfigApplied(t *testing.T) {
	t.Run("success despite masked password", func(t *testing.T) {
		client := &fakeHighLevelClient{
			getConfig: func(req kc.ConnectorRequest) (kc.GetConnectorConfigResponse, error) {
				if req.Name != "sqlite-sink" {
					t.Errorf("unexpected connector name %q", req.Name)
				}
				return kc.GetConnectorConfigResponse{
					Config: map[string]interface{}{
						"name":                      "sqlite-sink",
						"tasks.max":                 "1",
						"database.password":         "\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022\u2022",
						"__internal.config.version": "7",
					},
				}, nil
			},
		}
		desired := map[string]interface{}{
			"name":              "sqlite-sink",
			"tasks.max":         "1",
			"database.password": "s3cret",
		}
		err := waitForConfigApplied(client, "sqlite-sink", desired, nil, time.Second)
		if err != nil {
			t.Fatalf("expected success despite masked password, got %v", err)
		}
	})

	t.Run("rebalance error followed by success", func(t *testing.T) {
		calls := 0
		client := &fakeHighLevelClient{
			getConfig: func(req kc.ConnectorRequest) (kc.GetConnectorConfigResponse, error) {
				calls++
				if calls == 1 {
					return kc.GetConnectorConfigResponse{}, errors.New("rebalance in progress")
				}
				return kc.GetConnectorConfigResponse{
					Config: map[string]interface{}{
						"name":      "sqlite-sink",
						"tasks.max": "1",
					},
				}, nil
			},
		}
		desired := map[string]interface{}{
			"name":      "sqlite-sink",
			"tasks.max": "1",
		}
		start := time.Now()
		err := waitForConfigApplied(client, "sqlite-sink", desired, nil, 3*time.Second)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("expected success after rebalance, got %v", err)
		}
		if calls != 2 {
			t.Fatalf("expected 2 polls, got %d", calls)
		}
		if elapsed < time.Second {
			t.Fatalf("expected a poll interval before retry, elapsed %v", elapsed)
		}
	})

	t.Run("timeout lists mismatched key names", func(t *testing.T) {
		const secret = "super-secret-password"
		client := &fakeHighLevelClient{
			getConfig: func(req kc.ConnectorRequest) (kc.GetConnectorConfigResponse, error) {
				return kc.GetConnectorConfigResponse{
					Config: map[string]interface{}{
						"name":      "sqlite-sink",
						"tasks.max": "not-applied",
						"topics":    secret,
					},
				}, nil
			},
		}
		desired := map[string]interface{}{
			"name":      "sqlite-sink",
			"tasks.max": "1",
			"topics":    "orders",
		}
		err := waitForConfigApplied(client, "sqlite-sink", desired, nil, 20*time.Millisecond)
		if err == nil {
			t.Fatal("expected timeout error")
		}
		msg := err.Error()
		if !strings.Contains(msg, "connector sqlite-sink config not applied within") {
			t.Errorf("unexpected error %q", msg)
		}
		if !strings.Contains(msg, "mismatched keys: [tasks.max topics]") {
			t.Errorf("expected sorted key names in error, got %q", msg)
		}
		if strings.Contains(msg, secret) || strings.Contains(msg, "not-applied") || strings.Contains(msg, "orders") {
			t.Errorf("error must not include config values, got %q", msg)
		}
	})

	t.Run("non-rebalance error is returned immediately", func(t *testing.T) {
		calls := 0
		client := &fakeHighLevelClient{
			getConfig: func(req kc.ConnectorRequest) (kc.GetConnectorConfigResponse, error) {
				calls++
				return kc.GetConnectorConfigResponse{}, errors.New("connection timeout")
			},
		}
		start := time.Now()
		err := waitForConfigApplied(client, "sqlite-sink", map[string]interface{}{"name": "sqlite-sink"}, nil, 5*time.Second)
		if err == nil || err.Error() != "connection timeout" {
			t.Fatalf("expected connection timeout, got %v", err)
		}
		if calls != 1 {
			t.Fatalf("expected 1 poll, got %d", calls)
		}
		if time.Since(start) > time.Second {
			t.Fatalf("non-rebalance error should not wait, elapsed %v", time.Since(start))
		}
	})
}

func TestIsRebalanceError(t *testing.T) {
	rebalanceErr := errors.New("rebalance in progress")
	if !isRebalanceError(rebalanceErr) {
		t.Errorf("expected rebalance error to be detected")
	}

	normalErr := errors.New("connection timeout")
	if isRebalanceError(normalErr) {
		t.Errorf("expected normal error to not be detected as rebalance error")
	}
}

func TestWithRebalanceRetry(t *testing.T) {
	t.Run("successful operation after rebalance errors", func(t *testing.T) {
		callCount := 0
		operation := func() error {
			callCount++
			if callCount < 3 {
				return errors.New("rebalance in progress")
			}
			return nil
		}

		err := withRebalanceRetry(operation, 5*time.Second)
		if err != nil {
			t.Errorf("expected no error, got: %v", err)
		}
		if callCount != 3 {
			t.Errorf("expected 3 calls, got %d", callCount)
		}
	})

	t.Run("non-rebalance error should not retry", func(t *testing.T) {
		callCount := 0
		expectedErr := errors.New("connection timeout")
		operation := func() error {
			callCount++
			return expectedErr
		}

		err := withRebalanceRetry(operation, 5*time.Second)
		if err != expectedErr {
			t.Errorf("expected error %v, got: %v", expectedErr, err)
		}
		if callCount != 1 {
			t.Errorf("expected 1 call, got %d", callCount)
		}
	})

	t.Run("timeout should be respected", func(t *testing.T) {
		callCount := 0
		operation := func() error {
			callCount++
			// Always return rebalance error to force timeout
			return errors.New("rebalance in progress")
		}

		start := time.Now()
		err := withRebalanceRetry(operation, 1*time.Second)
		duration := time.Since(start)

		if err == nil {
			t.Errorf("expected timeout error, got nil")
		}
		if !errors.Is(err, errors.New("rebalance in progress")) {
			if err.Error() != "timed out waiting for Kafka Connect rebalance to finish: rebalance in progress" {
				t.Errorf("expected timeout error message, got: %v", err)
			}
		}
		// Verify timeout was respected (should be around 1s, allow generous margin for backoff and jitter)
		if duration < 800*time.Millisecond || duration > 2500*time.Millisecond {
			t.Errorf("expected timeout around 1s (with margin for backoff), got %v", duration)
		}
		if callCount < 2 {
			t.Errorf("expected at least 2 retry attempts, got %d", callCount)
		}
	})

	t.Run("long timeout allows many retries", func(t *testing.T) {
		callCount := 0
		operation := func() error {
			callCount++
			if callCount < 5 {
				return errors.New("rebalance in progress")
			}
			return nil
		}

		err := withRebalanceRetry(operation, 30*time.Second)
		if err != nil {
			t.Errorf("expected no error with long timeout, got: %v", err)
		}
		if callCount != 5 {
			t.Errorf("expected 5 calls, got %d", callCount)
		}
	})

	t.Run("very short timeout fails quickly", func(t *testing.T) {
		callCount := 0
		operation := func() error {
			callCount++
			// Add small delay to ensure timeout is hit
			time.Sleep(100 * time.Millisecond)
			return errors.New("rebalance in progress")
		}

		start := time.Now()
		err := withRebalanceRetry(operation, 50*time.Millisecond)
		duration := time.Since(start)

		if err == nil {
			t.Errorf("expected timeout error, got nil")
		}
		// With 50ms timeout and 100ms operation, should fail on first attempt
		if duration > 200*time.Millisecond {
			t.Errorf("expected fast timeout, got %v", duration)
		}
		if callCount != 1 {
			t.Errorf("expected 1 call with very short timeout, got %d", callCount)
		}
	})
}

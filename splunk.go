package splunk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/op/go-logging"
)

type Config struct {
	Event EventConfig `json:"event"`
	HEC   HECConfig   `json:"hec"`
}

type EventConfig struct {
	Index      string `json:"index"`
	Sourcetype string `json:"sourcetype"`
}

type HECConfig struct {
	URL           string `json:"url"`
	Port          string `json:"port"`
	Endpoint      string `json:"endpoint"`
	Token         string `json:"token"`
	MaxByteLength int    `json:"max_byte_length"`
}

type Event struct {
	Index      string      `json:"index"`
	Sourcetype string      `json:"sourcetype"`
	Source     string      `json:"source"`
	Host       string      `json:"host"`
	EventData  interface{} `json:"event"`
}

type Client struct {
	sync.Mutex
	HTTPClient        *http.Client
	BaseURL           *url.URL
	Endpoint          string
	Port              string
	Token             string
	MaxByteLength     int
	MaxHECPutRetries  int
	SSLVerify         bool
	BatchEvents       []*Event
	CurrentByteLength int
	Logger            *logging.Logger
}

type BatchEvents struct {
	sync.Mutex
	Events []*Event
}

func NewBatchEvents() *BatchEvents {
	return &BatchEvents{
		Events: nil,
	}
}

func NewClient(urlBase string, port string, endpoint string, tok string, maxByteLength int, maxHECPutRetries int, sslVerify bool) *Client {
	baseURL, _ := url.Parse(urlBase + ":" + port + "/" + endpoint)
	httpClientTimeout := time.Duration(30 * time.Second)

	return &Client{
		HTTPClient: &http.Client{
			Timeout: httpClientTimeout,
		},
		BaseURL:           baseURL,
		Token:             tok,
		BatchEvents:       nil,
		MaxHECPutRetries:  maxHECPutRetries,
		SSLVerify:         sslVerify,
		MaxByteLength:     maxByteLength,
		CurrentByteLength: 0,
	}
}

func (c *Client) BatchEvent(e *Event) error {
	c.Lock()
	defer c.Unlock()
	c.BatchEvents = append(c.BatchEvents, e)
	c.CurrentByteLength += int(unsafe.Sizeof(e))
	if c.CurrentByteLength > c.MaxByteLength {
		err := c.FlushBatch()
		if err != nil {
			return err
		}
		c.BatchEvents = nil
		c.CurrentByteLength = 0
	}
	return nil
}

func (c *Client) FlushBatch() error {
	var postData string
	for _, event := range c.BatchEvents {
		b, err := json.Marshal(event)
		if err != nil {
			return err
		}
		postData += string(b)
	}
	req, _ := http.NewRequest("POST", c.BaseURL.String(), bytes.NewBufferString(postData))
	req.Header.Add("Authorization", "Splunk "+c.Token)
	_, err := c.Do(req)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode > 299 || resp.StatusCode < 200 {
		if resp.StatusCode == 429 {
			retrySecs, _ := strconv.Atoi(resp.Header.Get("Retry-after"))
			time.Sleep(time.Duration(retrySecs) * time.Second)
			return nil, fmt.Errorf("rate limited for %d seconds", retrySecs)
		} else {
			return nil, fmt.Errorf("http request failed, resp: %#v", resp)
		}
	}
	return resp, err
}

package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/chenyao-lib/go/log"
)

type HttpClient struct {
	httpClient *http.Client
	encryptor  Encryptor
}

// Encryptor converts request bytes to a transport-safe string and restores
// response strings to their original bytes. A nil Encryptor means plaintext.
type Encryptor interface {
	Encrypt(plaintext []byte) (string, error)
	Decrypt(ciphertext string) ([]byte, error)
}

type apiRequest struct {
	Data      string `json:"data"`
	Lang      string `json:"lang"`
	Timestamp int64  `json:"timestamp"`
	Signature string `json:"signature"`
}

type apiResponse struct {
	Code      int    `json:"code"`
	Msg       string `json:"msg"`
	Data      string `json:"data"`
	Timestamp int64  `json:"timestamp"`
	Signature string `json:"signature"`
}

type APIError struct {
	Code int
	Msg  string
	Err  error
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return e.Msg + ": " + e.Err.Error()
	}
	return e.Msg
}

// NewHttpClient creates an API client. Pass nil as encryptor to send and
// receive plaintext JSON in the data field.
func NewHttpClient(timeoutSecs int, encryptor Encryptor) *HttpClient {
	if timeoutSecs <= 0 || timeoutSecs > 120 {
		timeoutSecs = 5
	}
	return &HttpClient{
		httpClient: &http.Client{Timeout: time.Duration(timeoutSecs) * time.Second},
		encryptor:  encryptor,
	}
}

// NewAesCipherClient creates a client using the built-in AES cipher.
func NewAesCipherClient(timeoutSecs int, base64Key string) (*HttpClient, error) {
	return NewHttpClient(timeoutSecs, NewAESCipher(base64Key)), nil
}

// Call sends a JSON request using the API data envelope and decodes its
// response into out.
func (c *HttpClient) Call(reqURL string, reqBody any, lang string, out any) error {
	startedAt := time.Now()
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal API request: %w", err)
	}
	data, err := c.encode(reqBytes)
	if err != nil {
		return fmt.Errorf("encode API request: %w", err)
	}
	apiResp, err := c.startRequest(reqURL, http.MethodPost, &apiRequest{
		Data:      data,
		Timestamp: time.Now().UnixNano(),
		Lang:      lang,
	})
	if err != nil {
		if apiResp == nil || apiResp.Data == "" {
			return err
		}
		if decErr := c.decryptData(apiResp.Data, out); decErr != nil {
			return err
		}
		return err
	}
	if apiResp == nil {
		return errors.New("API response is nil")
	}
	if apiResp.Code != 0 {
		if apiResp.Data != "" {
			_ = c.decryptData(apiResp.Data, out)
		}
		return &APIError{Code: apiResp.Code, Msg: apiResp.Msg}
	}
	if err := c.decryptData(apiResp.Data, out); err != nil {
		return err
	}
	log.Info("http api success: url=%s, cost=%s", reqURL, time.Since(startedAt))
	return nil
}

// CallFormMap sends reqBody in the __input form field and returns the decoded
// response data as a map.
func (c *HttpClient) CallFormMap(reqURL string, reqBody any) (map[string]any, error) {
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal form request: %w", err)
	}
	data, err := c.encode(reqBytes)
	if err != nil {
		return nil, fmt.Errorf("encode form request: %w", err)
	}
	respBody, err := c.requestURLEncoded(reqURL, map[string]string{"__input": data})
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data any    `json:"data"`
	}
	if err := json.Unmarshal(respBody, &wrapper); err != nil {
		return nil, fmt.Errorf("parse encrypted form wrapper response: %w", err)
	}
	if wrapper.Code != 0 {
		return nil, &APIError{Code: wrapper.Code, Msg: wrapper.Msg}
	}
	switch data := wrapper.Data.(type) {
	case nil:
		return map[string]any{}, nil
	case bool:
		return map[string]any{"isPrs": data}, nil
	case map[string]any:
		return data, nil
	case string:
		data = strings.TrimSpace(data)
		if data == "" {
			return map[string]any{}, nil
		}
		plain, err := c.decode(data)
		if err != nil {
			return nil, fmt.Errorf("decode form response: %w", err)
		}
		var out map[string]any
		if err := json.Unmarshal(plain, &out); err != nil {
			return nil, fmt.Errorf("parse encrypted form biz response: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected encrypted form response data type: %T", wrapper.Data)
	}
}

func (c *HttpClient) encode(data []byte) (string, error) {
	if c == nil || c.encryptor == nil {
		return string(data), nil
	}
	return c.encryptor.Encrypt(data)
}

func (c *HttpClient) decode(data string) ([]byte, error) {
	if c == nil || c.encryptor == nil {
		return []byte(data), nil
	}
	return c.encryptor.Decrypt(data)
}

func (c *HttpClient) requestURLEncoded(reqURL string, fields map[string]string) ([]byte, error) {
	form := url.Values{}
	for k, v := range fields {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, reqURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create urlencoded request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send urlencoded request: %w", err)
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("read urlencoded response: %w", readErr)
	}
	if resp.StatusCode != http.StatusOK {
		return respBody, fmt.Errorf("unexpected http status: %d", resp.StatusCode)
	}
	return respBody, nil
}

func (c *HttpClient) decryptData(data string, out any) error {
	decoded, err := c.decode(data)
	if err != nil {
		return fmt.Errorf("decode API response: %w", err)
	}
	if err := json.Unmarshal(decoded, out); err != nil {
		return fmt.Errorf("parse API response data: %w", err)
	}
	return nil
}

func (c *HttpClient) startRequest(reqURL string, reqMethod string, reqBody any) (*apiResponse, error) {
	respBytes, err := c.requestJSON(reqURL, reqMethod, reqBody)
	if err != nil {
		if len(respBytes) == 0 {
			return nil, err
		}
		var respObj apiResponse
		if jsonErr := json.Unmarshal(respBytes, &respObj); jsonErr != nil {
			return nil, err
		}
		return &respObj, errors.New(respObj.Msg)
	}
	var respObj apiResponse
	if err := json.Unmarshal(respBytes, &respObj); err != nil {
		return nil, fmt.Errorf("parse API response wrapper: %w", err)
	}
	return &respObj, nil
}

func (c *HttpClient) requestJSON(reqURL string, reqMethod string, reqBody any) ([]byte, error) {
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request body: %w", err)
	}
	req, err := http.NewRequest(reqMethod, reqURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("read response: %w", readErr)
	}
	if resp.StatusCode != http.StatusOK {
		return respBody, fmt.Errorf("unexpected http status: %d", resp.StatusCode)
	}
	return respBody, nil
}

// RequestForm sends a multipart form request and returns its raw response body.
func (c *HttpClient) RequestForm(reqURL string, fields map[string]string) ([]byte, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	for k, v := range fields {
		_ = writer.WriteField(k, v)
	}
	_ = writer.Close()
	req, err := http.NewRequest(http.MethodPost, reqURL, body)
	if err != nil {
		return nil, fmt.Errorf("create form request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send form request: %w", err)
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("read form response: %w", readErr)
	}
	if resp.StatusCode != http.StatusOK {
		return respBody, fmt.Errorf("unexpected http status: %d", resp.StatusCode)
	}
	return respBody, nil
}

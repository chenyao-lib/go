// Package httpapi 提供约定式 HTTP API 客户端，支持 JSON 数据信封、URL 编码
// 表单、multipart 表单以及可插拔的请求/响应加解密。
//
// # JSON API
//
//	type CreateRequest struct {
//		Name string `json:"name"`
//	}
//	type CreateResponse struct {
//		ID string `json:"id"`
//	}
//
//	client := httpapi.NewHttpClient(5, nil)
//	var result CreateResponse
//	err := client.Call(
//		"https://api.example.com/create",
//		CreateRequest{Name: "demo"},
//		"zh-CN",
//		&result,
//	)
//
// Call 会把业务请求编码到 data 字段，并附带 lang 和纳秒时间戳。服务端响应应为：
//
//	{"code":0,"msg":"","data":"...","timestamp":0,"signature":""}
//
// out 必须是可由 json.Unmarshal 写入的指针。code 非 0 时返回 *APIError；
// 可通过 errors.As 读取业务错误码：
//
//	var apiErr *httpapi.APIError
//	if errors.As(err, &apiErr) {
//		log.Warn("api error: code=%d, msg=%s", apiErr.Code, apiErr.Msg)
//	}
//
// timeoutSecs 不在 1 到 120 秒范围内时使用 5 秒。
//
// # 加密客户端
//
// 内置 AESCipher 使用 base64 编码的 16、24 或 32 字节 AES 密钥：
//
//	client, err := httpapi.NewAesCipherClient(5, base64Key)
//
// 也可实现 Encryptor 并传给 NewHttpClient。nil Encryptor 表示 data 字段直接
// 承载明文 JSON 字符串。内置实现用于兼容 AES-CBC+PKCS#7 协议；新协议若需要
// 防篡改能力，应优先使用带认证的加密设计。
//
// # 表单接口
//
// CallFormMap 将业务请求编码到 application/x-www-form-urlencoded 的 __input
// 字段，并将成功响应解析为 map[string]any。RequestForm 发送 multipart/form-data
// 字符串字段并返回原始响应体。两者都会把非 200 HTTP 状态作为错误返回。
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
	client := &HttpClient{
		httpClient: &http.Client{Timeout: time.Duration(timeoutSecs) * time.Second},
		encryptor:  encryptor,
	}
	log.Info("客户端创建完成: timeout=%ds, encryptor=%T", timeoutSecs, encryptor)
	return client
}

// NewAesCipherClient creates a client using the built-in AES cipher.
func NewAesCipherClient(timeoutSecs int, base64Key string) (*HttpClient, error) {
	return NewHttpClient(timeoutSecs, NewAESCipher(base64Key)), nil
}

// Call sends a JSON request using the API data envelope and decodes its
// response into out.
func (c *HttpClient) Call(reqURL string, reqBody any, lang string, out any) (callErr error) {
	startedAt := time.Now()
	defer func() {
		if callErr != nil {
			log.Error("JSON API 调用失败: url=%s, cost=%s, err=%v", reqURL, time.Since(startedAt), callErr)
		} else {
			log.Info("JSON API 调用成功: url=%s, cost=%s", reqURL, time.Since(startedAt))
		}
	}()

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
	return nil
}

// CallFormMap sends reqBody in the __input form field and returns the decoded
// response data as a map.
func (c *HttpClient) CallFormMap(reqURL string, reqBody any) (result map[string]any, callErr error) {
	startedAt := time.Now()
	defer func() {
		if callErr != nil {
			log.Error("URL 编码表单调用失败: url=%s, cost=%s, err=%v", reqURL, time.Since(startedAt), callErr)
		} else {
			log.Info("URL 编码表单调用成功: url=%s, cost=%s", reqURL, time.Since(startedAt))
		}
	}()

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
func (c *HttpClient) RequestForm(reqURL string, fields map[string]string) (result []byte, callErr error) {
	startedAt := time.Now()
	defer func() {
		if callErr != nil {
			log.Error("multipart 表单调用失败: url=%s, cost=%s, err=%v", reqURL, time.Since(startedAt), callErr)
		} else {
			log.Info("multipart 表单调用成功: url=%s, cost=%s", reqURL, time.Since(startedAt))
		}
	}()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	for k, v := range fields {
		if err := writer.WriteField(k, v); err != nil {
			return nil, fmt.Errorf("write form field %q: %w", k, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}
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

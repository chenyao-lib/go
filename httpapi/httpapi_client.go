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
// 防篡改能力，应优先使用带认证的加密设计。AES-CBC 本身不校验完整性，无法
// 发现密文被篡改；旧协议无法更换加密方式时，应设置 HttpClient.VerifyResponse
// 在解密前校验响应 timestamp/signature。
//
// # 表单接口
//
// CallFormMap 将业务请求编码到 application/x-www-form-urlencoded 的 __input
// 字段，并将包含整数 code 的成功响应解析为 map[string]any。RequestForm 发送 multipart/form-data
// 字符串字段并返回原始响应体。两者都会把非 200 HTTP 状态作为错误返回。
//
// 所有请求的响应体读取上限为 8 MiB，超出时返回错误，避免异常服务端耗尽内存。
package httpapi

import (
	"bytes"
	"context"
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

// maxResponseBytes 是单次响应体读取上限，防止异常或恶意服务端返回超大响应
// 导致客户端内存暴涨。
const maxResponseBytes = 8 << 20

type HttpClient struct {
	httpClient *http.Client
	encryptor  Encryptor

	// VerifyResponse 非 nil 时，Call 和 CallFormMap 会先校验业务响应，再处理业务码
	// 或返回数据。data 如果是 JSON 字符串，传入解码后的字符串；否则传入 data 的原始 JSON。
	// 返回错误会立即终止本次调用。RequestForm 返回原始响应体，由调用方校验。
	// 验签失败时，返回的错误也保留已接收的非 200 HTTP 状态。
	VerifyResponse func(data string, ts int64, sig string) error
}

// HTTPStatusError 表示服务端返回了非 200 HTTP 状态码。
// Body 保留完整的响应正文。正文读取失败时，返回的错误同时包含读取错误。
type HTTPStatusError struct {
	StatusCode int
	Body       []byte
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("unexpected http status: %d", e.StatusCode)
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

// NewAesCipherClient creates a client using the built-in AES cipher and
// returns an error if the key cannot be used for AES.
func NewAesCipherClient(timeoutSecs int, base64Key string) (*HttpClient, error) {
	encryptor := NewAESCipher(base64Key)
	if _, err := encryptor.key(); err != nil {
		return nil, err
	}
	return NewHttpClient(timeoutSecs, encryptor), nil
}

// Call sends a JSON request using the API data envelope and decodes its
// response into out.
func (c *HttpClient) Call(reqURL string, reqBody any, lang string, out any) error {
	return c.CallContext(context.Background(), reqURL, reqBody, lang, out)
}

// CallContext is like Call, with request cancellation and deadlines from ctx.
func (c *HttpClient) CallContext(ctx context.Context, reqURL string, reqBody any, lang string, out any) (callErr error) {
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
	apiResp, err := c.startRequest(ctx, reqURL, http.MethodPost, &apiRequest{
		Data:      data,
		Timestamp: time.Now().UnixNano(),
		Lang:      lang,
	})
	if apiResp != nil {
		if verifyErr := c.verifyResponse(apiResp.Data, apiResp.Timestamp, apiResp.Signature); verifyErr != nil {
			return retainHTTPStatus(err, verifyErr)
		}
	}
	if err != nil {
		if apiResp != nil && apiResp.Data != "" {
			_ = c.decryptData(apiResp.Data, out)
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

// verifyResponse 在解密前校验响应签名；未配置 VerifyResponse 时视为通过。
func (c *HttpClient) verifyResponse(data string, timestamp int64, signature string) error {
	if c == nil || c.VerifyResponse == nil {
		return nil
	}
	if err := c.VerifyResponse(data, timestamp, signature); err != nil {
		return fmt.Errorf("verify API response signature: %w", err)
	}
	return nil
}

func retainHTTPStatus(requestErr, responseErr error) error {
	var status *HTTPStatusError
	if errors.As(requestErr, &status) {
		// The request error may also include a business message whose signature failed.
		return errors.Join(status, responseErr)
	}
	return responseErr
}

// CallFormMap sends reqBody in the __input form field and returns the decoded
// response data as a map.
func (c *HttpClient) CallFormMap(reqURL string, reqBody any) (map[string]any, error) {
	return c.CallFormMapContext(context.Background(), reqURL, reqBody)
}

// CallFormMapContext is like CallFormMap, with request cancellation and deadlines from ctx.
func (c *HttpClient) CallFormMapContext(ctx context.Context, reqURL string, reqBody any) (result map[string]any, callErr error) {
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
	respBody, requestErr := c.requestURLEncoded(ctx, reqURL, map[string]string{"__input": data})
	if requestErr != nil && (len(respBody) == 0 || c.VerifyResponse == nil) {
		return nil, requestErr
	}
	var wrapper struct {
		Code      *int            `json:"code"`
		Msg       string          `json:"msg"`
		Data      json.RawMessage `json:"data"`
		Timestamp json.RawMessage `json:"timestamp"`
		Signature json.RawMessage `json:"signature"`
	}
	if err := json.Unmarshal(respBody, &wrapper); err != nil {
		if requestErr != nil {
			return nil, requestErr
		}
		return nil, fmt.Errorf("parse encrypted form wrapper response: %w", err)
	}
	if c.VerifyResponse != nil {
		data := string(wrapper.Data)
		if len(wrapper.Data) > 0 && wrapper.Data[0] == '"' {
			if err := json.Unmarshal(wrapper.Data, &data); err != nil {
				return nil, retainHTTPStatus(requestErr, fmt.Errorf("parse form response data: %w", err))
			}
		}
		var timestamp int64
		if len(wrapper.Timestamp) != 0 {
			if err := json.Unmarshal(wrapper.Timestamp, &timestamp); err != nil {
				return nil, retainHTTPStatus(requestErr, fmt.Errorf("parse form response timestamp: %w", err))
			}
		}
		var signature string
		if len(wrapper.Signature) != 0 {
			if err := json.Unmarshal(wrapper.Signature, &signature); err != nil {
				return nil, retainHTTPStatus(requestErr, fmt.Errorf("parse form response signature: %w", err))
			}
		}
		if err := c.verifyResponse(data, timestamp, signature); err != nil {
			return nil, retainHTTPStatus(requestErr, err)
		}
	}
	if requestErr != nil {
		return nil, requestErr
	}
	if wrapper.Code == nil {
		return nil, errors.New("form response is missing a business code")
	}
	if *wrapper.Code != 0 {
		return nil, &APIError{Code: *wrapper.Code, Msg: wrapper.Msg}
	}
	var value any
	if len(wrapper.Data) != 0 {
		if err := json.Unmarshal(wrapper.Data, &value); err != nil {
			return nil, fmt.Errorf("parse form response data: %w", err)
		}
	}
	switch data := value.(type) {
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

func (c *HttpClient) requestURLEncoded(ctx context.Context, reqURL string, fields map[string]string) ([]byte, error) {
	form := url.Values{}
	for k, v := range fields {
		form.Set(k, v)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create urlencoded request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send urlencoded request: %w", err)
	}
	return readHTTPResponse(resp, "urlencoded response")
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

func (c *HttpClient) startRequest(ctx context.Context, reqURL string, reqMethod string, reqBody any) (*apiResponse, error) {
	respBytes, err := c.requestJSON(ctx, reqURL, reqMethod, reqBody)
	if err != nil {
		if len(respBytes) == 0 {
			return nil, err
		}
		var respObj apiResponse
		if jsonErr := json.Unmarshal(respBytes, &respObj); jsonErr != nil {
			return nil, err
		}
		// 保留原始错误（含 *HTTPStatusError 的状态码），只补充服务端 msg，
		// 避免把状态码信息替换成纯业务文案后无法区分 401/500。
		if respObj.Msg != "" {
			return &respObj, fmt.Errorf("%w: %s", err, respObj.Msg)
		}
		return &respObj, err
	}
	var respObj apiResponse
	if err := json.Unmarshal(respBytes, &respObj); err != nil {
		return nil, fmt.Errorf("parse API response wrapper: %w", err)
	}
	return &respObj, nil
}

func (c *HttpClient) requestJSON(ctx context.Context, reqURL string, reqMethod string, reqBody any) ([]byte, error) {
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, reqMethod, reqURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	return readHTTPResponse(resp, "response")
}

// RequestForm sends a multipart form request and returns its raw response body.
func (c *HttpClient) RequestForm(reqURL string, fields map[string]string) ([]byte, error) {
	return c.RequestFormContext(context.Background(), reqURL, fields)
}

// RequestFormContext is like RequestForm, with request cancellation and deadlines from ctx.
func (c *HttpClient) RequestFormContext(ctx context.Context, reqURL string, fields map[string]string) (result []byte, callErr error) {
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, body)
	if err != nil {
		return nil, fmt.Errorf("create form request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send form request: %w", err)
	}
	return readHTTPResponse(resp, "form response")
}

func readHTTPResponse(resp *http.Response, description string) ([]byte, error) {
	defer resp.Body.Close()
	body, readErr := readResponseBody(resp.Body)
	if resp.StatusCode != http.StatusOK {
		statusErr := &HTTPStatusError{StatusCode: resp.StatusCode, Body: body}
		if readErr != nil {
			return nil, errors.Join(statusErr, fmt.Errorf("read %s: %w", description, readErr))
		}
		return body, statusErr
	}
	if readErr != nil {
		return nil, fmt.Errorf("read %s: %w", description, readErr)
	}
	return body, nil
}

// readResponseBody 读取响应体，超过 maxResponseBytes 时返回错误，
// 而不是把超大响应读进内存。
func readResponseBody(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxResponseBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes", maxResponseBytes)
	}
	return data, nil
}

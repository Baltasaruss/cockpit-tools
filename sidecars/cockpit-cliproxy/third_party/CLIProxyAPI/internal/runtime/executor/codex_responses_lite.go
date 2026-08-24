package executor

import (
	"fmt"
	"net/http"
	"strings"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const codexResponsesLiteHeaderName = "X-OpenAI-Internal-Codex-Responses-Lite"

func codexResponsesLiteEnabled(headers http.Header) bool {
	for name := range headers {
		if strings.EqualFold(name, codexResponsesLiteHeaderName) {
			return true
		}
	}
	return false
}

func removeCodexResponsesLiteHeaderForFullResponse(headers http.Header, full bool) {
	if !full || headers == nil {
		return
	}
	for name := range headers {
		if strings.EqualFold(name, codexResponsesLiteHeaderName) {
			delete(headers, name)
		}
	}
}

func normalizeCodexResponsesLiteRequest(body []byte, headers http.Header, auth *cliproxyauth.Auth, allowFullResponsesForImage bool) ([]byte, bool) {
	if !codexResponsesLiteEnabled(headers) || auth == nil || codexAuthUsesAPIKey(auth) {
		return body, false
	}
	tools := gjson.GetBytes(body, "tools")
	if !tools.IsArray() {
		tools = gjson.GetBytes(body, "input.0.tools")
	}
	if !tools.IsArray() {
		return body, false
	}
	full := allowFullResponsesForImage && containsCodexImageTool(tools)
	if allowFullResponsesForImage && tools.IsArray() && full == false && containsCodexImageTool(tools) {
		full = true
	}
	if full {
		body, _ = sjson.SetBytes(body, "parallel_tool_calls", false)
		return body, true
	}
	filtered := make([][]byte, 0, len(tools.Array()))
	for _, tool := range tools.Array() {
		if codexResponsesLiteToolSupported(tool) {
			raw := []byte(tool.Raw)
			filtered = append(filtered, raw)
		}
	}
	if len(filtered) == 0 {
		body, _ = sjson.DeleteBytes(body, "tools")
	} else {
		body, _ = sjson.SetRawBytes(body, "tools", joinJSONArray(filtered))
	}
	choice := gjson.GetBytes(body, "tool_choice")
	if choice.Exists() && strings.EqualFold(strings.TrimSpace(choice.String()), "required") {
		// required remains valid for the filtered collaboration/function tool set.
	} else {
		body, _ = sjson.DeleteBytes(body, "tool_choice")
	}
	body, _ = sjson.SetBytes(body, "parallel_tool_calls", false)
	return body, false
}

func joinJSONArray(items [][]byte) []byte {
	if len(items) == 0 {
		return []byte("[]")
	}
	out := []byte("[")
	for index, item := range items {
		if index > 0 {
			out = append(out, ',')
		}
		out = append(out, item...)
	}
	return append(out, ']')
}

func codexResponsesLiteToolSupported(tool gjson.Result) bool {
	switch strings.TrimSpace(tool.Get("type").String()) {
	case "function", "custom", "namespace":
		return true
	case "tool_search":
		return strings.EqualFold(strings.TrimSpace(tool.Get("execution").String()), "client")
	default:
		return false
	}
}

func containsCodexImageTool(tools gjson.Result) bool {
	for _, tool := range tools.Array() {
		if tool.Get("type").String() == "image_generation" || isCodexImageFunction(tool) {
			return true
		}
	}
	return false
}

func isCodexImageFunction(tool gjson.Result) bool {
	name := tool.Get("name").String()
	if name == "image_gen.imagegen" {
		return true
	}
	if name != "image_gen" || !tool.Get("tools").IsArray() {
		return false
	}
	for _, child := range tool.Get("tools").Array() {
		if child.Get("name").String() == "imagegen" {
			return true
		}
	}
	return false
}

var _ = fmt.Sprintf

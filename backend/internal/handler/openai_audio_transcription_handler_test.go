package handler

import (
	"bytes"
	"mime/multipart"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateCodexTranscriptionMultipart_AcceptsNativeShape(t *testing.T) {
	body, contentType := buildHandlerTranscriptionMultipart(t, "audio.wav", map[string]string{
		"model":           effectiveTranscriptionModel,
		"response_format": "json",
		"stream":          "false",
		"language":        "zh",
		"prompt":          "previous context",
		"temperature":     "0",
	})

	require.NoError(t, validateCodexTranscriptionMultipart(body, contentType))
}

func TestValidateCodexTranscriptionMultipart_AcceptsCompatModelAliases(t *testing.T) {
	for _, model := range []string{
		"gpt-4o-mini-transcribe",
		"gpt-4o-mini-transcribe-2025-03-20",
		"gpt-4o-mini-transcribe-2025-12-15",
		"whisper-1",
	} {
		t.Run(model, func(t *testing.T) {
			body, contentType := buildHandlerTranscriptionMultipart(t, "audio.wav", map[string]string{
				"model": model,
			})

			require.NoError(t, validateCodexTranscriptionMultipart(body, contentType))
			billingModel, ok := normalizeCodexTranscriptionModel(model)
			require.True(t, ok)
			require.Equal(t, effectiveTranscriptionModel, billingModel)
		})
	}
}

func TestValidateCodexTranscriptionMultipart_RejectsUnsupportedOpenAIParams(t *testing.T) {
	tests := []struct {
		name    string
		fields  map[string]string
		wantErr string
	}{
		{
			name:    "logprobs include",
			fields:  map[string]string{"include[]": "logprobs"},
			wantErr: "parameter \"include[]\" is not supported",
		},
		{
			name:    "unsupported model",
			fields:  map[string]string{"model": "gpt-4o-transcribe-diarize"},
			wantErr: "model \"gpt-4o-transcribe-diarize\" is not supported",
		},
		{
			name:    "streaming",
			fields:  map[string]string{"stream": "true"},
			wantErr: "stream is not supported",
		},
		{
			name:    "non json response format",
			fields:  map[string]string{"response_format": "verbose_json"},
			wantErr: "response_format \"verbose_json\" is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, contentType := buildHandlerTranscriptionMultipart(t, "audio.wav", tt.fields)

			err := validateCodexTranscriptionMultipart(body, contentType)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestValidateCodexTranscriptionMultipart_RejectsMissingFile(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	require.NoError(t, writer.WriteField("model", effectiveTranscriptionModel))
	require.NoError(t, writer.Close())

	err := validateCodexTranscriptionMultipart(body.Bytes(), writer.FormDataContentType())
	require.ErrorContains(t, err, "missing file field")
}

func buildHandlerTranscriptionMultipart(t *testing.T, fileName string, fields map[string]string) ([]byte, string) {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	filePart, err := writer.CreateFormFile("file", fileName)
	require.NoError(t, err)
	_, err = filePart.Write([]byte("abc123"))
	require.NoError(t, err)
	for key, value := range fields {
		require.NoError(t, writer.WriteField(key, value))
	}
	require.NoError(t, writer.Close())
	return body.Bytes(), writer.FormDataContentType()
}

// Code generated by easyjson for marshaling/unmarshaling. DO NOT EDIT.

package analytics

import (
	json "encoding/json"
	easyjson "github.com/mailru/easyjson"
	jlexer "github.com/mailru/easyjson/jlexer"
	jwriter "github.com/mailru/easyjson/jwriter"
)

// suppress unused package warning
var (
	_ *json.RawMessage
	_ *jlexer.Lexer
	_ *jwriter.Writer
	_ easyjson.Marshaler
)

func easyjson3a4fd032DecodeGithubComSnowplowSnowplowGolangAnalyticsSdkAnalytics(in *jlexer.Lexer, out *MapStringInterface) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		in.Skip()
	} else {
		in.Delim('{')
		*out = make(MapStringInterface)
		for !in.IsDelim('}') {
			key := string(in.String())
			in.WantColon()
			var v1 interface{}
			if m, ok := v1.(easyjson.Unmarshaler); ok {
				m.UnmarshalEasyJSON(in)
			} else if m, ok := v1.(json.Unmarshaler); ok {
				_ = m.UnmarshalJSON(in.Raw())
			} else {
				v1 = in.Interface()
			}
			(*out)[key] = v1
			in.WantComma()
		}
		in.Delim('}')
	}
	if isTopLevel {
		in.Consumed()
	}
}
func easyjson3a4fd032EncodeGithubComSnowplowSnowplowGolangAnalyticsSdkAnalytics(out *jwriter.Writer, in MapStringInterface) {
	if in == nil && (out.Flags&jwriter.NilMapAsEmpty) == 0 {
		out.RawString(`null`)
	} else {
		out.RawByte('{')
		v2First := true
		for v2Name, v2Value := range in {
			if v2First {
				v2First = false
			} else {
				out.RawByte(',')
			}
			out.String(string(v2Name))
			out.RawByte(':')
			if m, ok := v2Value.(easyjson.Marshaler); ok {
				m.MarshalEasyJSON(out)
			} else if m, ok := v2Value.(json.Marshaler); ok {
				out.Raw(m.MarshalJSON())
			} else {
				out.Raw(json.Marshal(v2Value))
			}
		}
		out.RawByte('}')
	}
}

// MarshalJSON supports json.Marshaler interface
func (v MapStringInterface) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjson3a4fd032EncodeGithubComSnowplowSnowplowGolangAnalyticsSdkAnalytics(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v MapStringInterface) MarshalEasyJSON(w *jwriter.Writer) {
	easyjson3a4fd032EncodeGithubComSnowplowSnowplowGolangAnalyticsSdkAnalytics(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *MapStringInterface) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjson3a4fd032DecodeGithubComSnowplowSnowplowGolangAnalyticsSdkAnalytics(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *MapStringInterface) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjson3a4fd032DecodeGithubComSnowplowSnowplowGolangAnalyticsSdkAnalytics(l, v)
}

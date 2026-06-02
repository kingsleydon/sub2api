package mimeheader

import "net/textproto"

// Clone copies MIME header values into a new map.
func Clone(h textproto.MIMEHeader) textproto.MIMEHeader {
	out := make(textproto.MIMEHeader, len(h))
	for k, values := range h {
		cloned := make([]string, len(values))
		copy(cloned, values)
		out[k] = cloned
	}
	return out
}

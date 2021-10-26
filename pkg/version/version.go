/*
 * Copyright 2021 Johan Fleury <jfleury@arcaik.net>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package version

import (
	"bytes"
	"runtime"
	"text/template"
)

// Build information populated at build-time.
var (
	Version   string
	Revision  string
	Branch    string
	BuildUser string
	BuildDate string
	GoVersion = runtime.Version()
)

var versionInfoTmpl = `{{ printf "{{ .Name }}" }}, version {{ .Version }}
  Branch:         {{ .Branch }}
  Revision:       {{ .Revision }}
  Go version:     {{ .GoVersion }}
  Platform:       {{ .Platform }}
  Build user:     {{ .BuildUser }}
  Build date:     {{ .BuildDate }}
`

// Template returns a template string to be used for printing version
// informations.
func Template() string {
	m := map[string]string{
		"Version":   Version,
		"Revision":  Revision,
		"Branch":    Branch,
		"BuildUser": BuildUser,
		"BuildDate": BuildDate,
		"GoVersion": GoVersion,
		"Platform":  runtime.GOOS + "/" + runtime.GOARCH,
	}

	t := template.Must(template.New("version").Parse(versionInfoTmpl))

	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "version", m); err != nil {
		panic(err)
	}

	return buf.String()
}

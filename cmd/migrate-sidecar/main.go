// Copyright 2026 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/versity/versitygw/backend/meta"
)

func main() {
	sidecarDir := flag.String("sidecar", "", "移行元の sidecar メタデータディレクトリ (必須)")
	metadbDir := flag.String("metadb", "", "移行先の SQLite メタデータディレクトリ。事前に存在している必要がある (必須)")
	apply := flag.Bool("apply", false, "実際に書き込む。指定しなければ何が移行されるか数えるだけの dry-run")
	verbose := flag.Bool("verbose", false, "(bucket, object) ごとの進捗を表示する")
	flag.Parse()

	if *sidecarDir == "" || *metadbDir == "" {
		fmt.Fprintln(os.Stderr, "使い方: migrate-sidecar --sidecar <dir> --metadb <dir> [--apply] [--verbose]")
		flag.PrintDefaults()
		os.Exit(2)
	}

	src, err := meta.NewSideCar(*sidecarDir)
	if err != nil {
		log.Fatalf("sidecar を開けません (%s): %v", *sidecarDir, err)
	}

	dst, err := meta.NewSQLite(*metadbDir)
	if err != nil {
		log.Fatalf("metadb を開けません (%s): %v", *metadbDir, err)
	}
	defer dst.Close()

	if !*apply {
		fmt.Println("dry-run モード (--apply 無し)。何も書き込みません。")
	}

	var progress func(e entry, attrs int)
	if *verbose {
		progress = func(e entry, attrs int) {
			obj := e.object
			if obj == "" {
				obj = "(bucket-level)"
			}
			fmt.Printf("  %s/%s: %d 属性\n", e.bucket, obj, attrs)
		}
	}

	res, err := migrate(src, dst, *sidecarDir, !*apply, progress)
	if err != nil {
		log.Fatalf("移行に失敗しました: %v", err)
	}

	verb := "移行しました"
	if !*apply {
		verb = "移行対象として検出しました (dry-run)"
	}
	fmt.Printf("%d 件の (bucket, object)、計 %d 属性を%s。\n", res.entries, res.attributes, verb)
}

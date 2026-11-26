/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/mit
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict'
var helper = require('./test-helper')
var assert = require('assert')

test('COPY FROM events check', function () {
  helper.connect(function (con) {
    var stdinStream = con.query('COPY person FROM STDIN')
    con.on('copyInResponse', function () {
      con.endCopyFrom()
    })
    assert.emits(
      con,
      'copyInResponse',
      function () {
        con.endCopyFrom()
      },
      'backend should  emit copyInResponse after COPY FROM query'
    )
    assert.emits(
      con,
      'commandComplete',
      function () {
        con.end()
      },
      'backend should emit commandComplete after COPY FROM stream ends'
    )
  })
})
test('COPY TO events check', function () {
  helper.connect(function (con) {
    var stdoutStream = con.query('COPY person TO STDOUT')
    assert.emits(con, 'copyOutResponse', function () {}, 'backend should emit copyOutResponse after COPY TO query')
    assert.emits(con, 'copyData', function () {}, 'backend should emit copyData on every data row')
    assert.emits(
      con,
      'copyDone',
      function () {
        con.end()
      },
      'backend should emit copyDone after all data rows'
    )
  })
})

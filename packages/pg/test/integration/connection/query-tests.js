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

var rows = []
// testing the low level 1-1 mapping api of client to postgres messages
// it's cumbersome to use the api this way
test('simple query', function () {
  helper.connect(function (con) {
    con.query('select * from ids')
    assert.emits(con, 'dataRow')
    con.on('dataRow', function (msg) {
      rows.push(msg.fields)
    })
    assert.emits(con, 'readyForQuery', function () {
      con.end()
    })
  })
})

process.on('exit', function () {
  assert.equal(rows.length, 2)
  assert.equal(rows[0].length, 1)
  assert.strictEqual(String(rows[0][0]), '1')
  assert.strictEqual(String(rows[1][0]), '2')
})

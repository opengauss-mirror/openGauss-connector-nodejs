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

var Client = helper.Client

var conInfo = helper.config

var checkResult = function (result) {
  assert(result.fields)
  assert.equal(result.fields.length, 3)
  var fields = result.fields
  assert.equal(fields[0].name, 'now')
  assert.equal(fields[1].name, 'num')
  assert.equal(fields[2].name, 'texty')
  assert.equal(fields[0].dataTypeID, 1184)
  assert.equal(fields[1].dataTypeID, 23)
  assert.equal(fields[2].dataTypeID, 25)
}

test('row descriptions on result object', function () {
  var client = new Client(conInfo)
  client.connect(
    assert.success(function () {
      client.query(
        'SELECT NOW() as now, 1::int as num, $1::text as texty',
        ['hello'],
        assert.success(function (result) {
          checkResult(result)
          client.end()
        })
      )
    })
  )
})

test('row description on no rows', function () {
  var client = new Client(conInfo)
  client.connect(
    assert.success(function () {
      client.query(
        'SELECT NOW() as now, 1::int as num, $1::text as texty LIMIT 0',
        ['hello'],
        assert.success(function (result) {
          checkResult(result)
          client.end()
        })
      )
    })
  )
})

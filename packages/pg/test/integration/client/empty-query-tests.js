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
const suite = new helper.Suite()

suite.test('empty query message handling', function (done) {
  const client = helper.client()
  assert.emits(client, 'drain', function () {
    client.end(done)
  })
  client.query({ text: '' })
})

suite.test('callback supported', function (done) {
  const client = helper.client()
  client.query('', function (err, result) {
    assert(!err)
    assert.empty(result.rows)
    client.end(done)
  })
})

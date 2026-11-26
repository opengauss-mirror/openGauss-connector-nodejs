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
var helper = require('./../test-helper')
var assert = require('assert')

const suite = new helper.Suite()

suite.test('All queries should return a result array', (done) => {
  const client = new helper.pg.Client()
  client.connect()
  const promises = []
  promises.push(client.query('CREATE TEMP TABLE foo(bar TEXT)'))
  promises.push(client.query('INSERT INTO foo(bar) VALUES($1)', ['qux']))
  promises.push(client.query('SELECT * FROM foo WHERE bar = $1', ['foo']))
  Promise.all(promises).then((results) => {
    results.forEach((res) => {
      assert(Array.isArray(res.fields))
      assert(Array.isArray(res.rows))
    })
    client.end(done)
  })
})

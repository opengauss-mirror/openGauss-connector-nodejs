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

const assert = require('assert')
const pg = require('pg')
const Cursor = require('../')

describe('queries with no data', function () {
  beforeEach(function (done) {
    const client = (this.client = new pg.Client())
    client.connect(done)
  })

  afterEach(function () {
    this.client.end()
  })

  it('handles queries that return no data', function (done) {
    const cursor = new Cursor('CREATE TEMPORARY TABLE whatwhat (thing int)')
    this.client.query(cursor)
    cursor.read(100, function (err, rows) {
      assert.ifError(err)
      assert.strictEqual(rows.length, 0)
      done()
    })
  })

  it('handles empty query', function (done) {
    let cursor = new Cursor('-- this is a comment')
    cursor = this.client.query(cursor)
    cursor.read(100, function (err, rows) {
      assert.ifError(err)
      assert.strictEqual(rows.length, 0)
      done()
    })
  })
})
